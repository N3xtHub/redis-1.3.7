// replication.c

/* =============================== Replication  ============================= */

static int syncWrite(int fd, char *ptr, ssize_t size, int timeout) {
    ssize_t nwritten, ret = size;
    time_t start = time(NULL);

    timeout++;
    while(size) {
        if (aeWait(fd, AE_WRITABLE, 1000) & AE_WRITABLE) {
            nwritten = write(fd, ptr,size);
            ptr += nwritten;
            size -= nwritten;
        }
    }

    return ret;
}

static int syncRead(int fd, char *ptr, ssize_t size, int timeout) {
    ssize_t nread, totread = 0;
    time_t start = time(NULL);

    timeout++;
    while(size) {
        nread = read(fd, ptr,size);
        ptr += nread;
        size -= nread;
        totread += nread;
    }

    return totread;
}

static int syncReadLine(int fd, char *ptr, ssize_t size, int timeout) {
    ssize_t nread = 0;

    size--;
    while(size) {
        char c;

        syncRead(fd, &c, 1, timeout);
        if (c == '\n') {
            *ptr = '\0';
            if (nread && *(ptr-1) == '\r') *(ptr-1) = '\0';
            return nread;
        } else {
            *ptr++ = c;
            *ptr = '\0';
            nread++;
        }
    }

    return nread;
}

static void syncCommand(redisClient *c) {

    /* ignore SYNC if aleady slave or in monitor mode */
    if (c->flags & REDIS_SLAVE) return;

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other slaves if needed. */
    if (listLength(c->reply) != 0) {
        addReplySds(c,sdsnew("-ERR SYNC is invalid with pending input\r\n"));
        return;
    }

    redisLog(REDIS_NOTICE,"Slave ask for synchronization");
    /* Here we need to check if there is a background saving operation
     * in progress, or if it is required to start one */
    if (server.bgsavechildpid != -1) {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save */
        redisClient *slave;
        listNode *ln;
        listIter li;

        listRewind(server.slaves, &li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) break;
        }
        if (ln) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer. */
            listRelease(c->reply);
            c->reply = listDup(slave->reply);
            c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
            redisLog(REDIS_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences */
            c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
            redisLog(REDIS_NOTICE,"Waiting for next BGSAVE for SYNC");
        }
    } 
    else {
        /* Ok we don't have a BGSAVE in progress, let's start one */
        redisLog(REDIS_NOTICE,"Starting BGSAVE for SYNC");
        rdbSaveBackground(server.dbfilename) ;
        c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
    }

    c->repldbfd = -1;
    c->flags |= REDIS_SLAVE;
    c->slaveseldb = 0;
    listAddNodeTail(server.slaves,c);
    return;
}

static void sendBulkToSlave(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *slave = privdata;
    char buf[REDIS_IOBUF_LEN];
    ssize_t nwritten, buflen;

    if (slave->repldboff == 0) {
        /* Write the bulk write count before to transfer the DB. In theory here
         * we don't know how much room there is in the output buffer of the
         * socket, but in pratice SO_SNDLOWAT (the minimum count for output
         * operations) will never be smaller than the few bytes we need. */
        sds bulkcount;

        bulkcount = sdscatprintf(sdsempty(), "$%lld\r\n", (unsigned long long)
            slave->repldbsize);
        write(fd,bulkcount, sdslen(bulkcount));
    }

    lseek(slave->repldbfd,slave->repldboff,SEEK_SET);
    buflen = read(slave->repldbfd,buf,REDIS_IOBUF_LEN);

    nwritten = write(fd,buf,buflen);

    slave->repldboff += nwritten;
    if (slave->repldboff == slave->repldbsize) {
        close(slave->repldbfd);
        slave->repldbfd = -1;
        aeDeleteFileEvent(server.el, slave->fd, AE_WRITABLE);
        slave->replstate = REDIS_REPL_ONLINE;
        aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, sendReplyToClient, slave);

        addReplySds(slave,sdsempty());
    }
}

/* This function is called at the end of every backgrond saving.
 * The argument bgsaveerr is REDIS_OK if the background saving succeeded
 * otherwise REDIS_ERR is passed to the function.
 *
 * The goal of this function is to handle slaves waiting for a successful
 * background saving in order to perform non-blocking synchronization. */
static void updateSlavesWaitingBgsave(int bgsaveerr) {
    listNode *ln;
    int startbgsave = 0;
    listIter li;

    listRewind(server.slaves, &li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) {
            startbgsave = 1;
            slave->replstate = REDIS_REPL_WAIT_BGSAVE_END;
        } 
        else if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            slave->repldbfd = open(server.dbfilename, O_RDONLY);

            redis_fstat(slave->repldbfd, &buf) ;

            slave->repldboff = 0;
            slave->repldbsize = buf.st_size;
            slave->replstate = REDIS_REPL_SEND_BULK;
            aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);
            aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, sendBulkToSlave, slave) ;
        }
    }

    if (startbgsave) {
        rdbSaveBackground(server.dbfilename);
    }
}

static int syncWithMaster(void) {
    char buf[1024], tmpfile[256], authcmd[1024];
    long dumpsize;
    int fd = anetTcpConnect(NULL,  server.masterhost, server.masterport);
    int dfd, maxtries = 5;

    /* AUTH with the master if required. */
    if(server.masterauth) {
    	snprintf(authcmd, 1024, "AUTH %s\r\n", server.masterauth);
    	syncWrite(fd, authcmd, strlen(server.masterauth)+7, 5);

        /* Read the AUTH result.  */
        syncReadLine(fd,buf,1024,3600) ;

        if (buf[0] != '+') {
            close(fd);
            return REDIS_ERR;
        }
    }

    /* Issue the SYNC command */
    syncWrite(fd, "SYNC \r\n", 7, 5) ;
    syncReadLine(fd, buf, 1024, 3600);

    if (buf[0] != '$') {
        close(fd);
        redisLog(REDIS_WARNING,"Bad protocol from MASTER, the first byte is not '$', are you sure the host and port are right?");
        return REDIS_ERR;
    }
    
    dumpsize = strtol(buf+1, NULL, 10);
    redisLog(REDIS_NOTICE,"Receiving %ld bytes data dump from MASTER",dumpsize);

    /* Read the bulk write data on a temp file */
	snprintf(tmpfile, 256, "temp-%d.%ld.rdb", (int)time(NULL), (long int)getpid());
    dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
    
    while(dumpsize) {
        int nread, nwritten;

        nread = read(fd,buf,(dumpsize < 1024)?dumpsize:1024);
        nwritten = write(dfd,buf,nread);      
        dumpsize -= nread;
    }
    close(dfd);

    rename(tmpfile, server.dbfilename);

    emptyDb();
    rdbLoad(server.dbfilename);

    server.master = createClient(fd);
    server.master->flags |= REDIS_MASTER;
    server.master->authenticated = 1;
    server.replstate = REDIS_REPL_CONNECTED;

    return REDIS_OK;
}

static void slaveofCommand(redisClient *c) {
    if (c->argv[1] == "no" && c->argv[2] == "one") {
        if (server.masterhost) {
            sdsfree(server.masterhost);
            server.masterhost = NULL;
            if (server.master) freeClient(server.master);
            server.replstate = REDIS_REPL_NONE;

            redisLog(REDIS_NOTICE,"MASTER MODE enabled (user request)");
        }
    } 
    else {
        
        server.masterhost = sdsdup(c->argv[1]->ptr);
        server.masterport = atoi(c->argv[2]->ptr);
        if (server.master) freeClient(server.master);
        server.replstate = REDIS_REPL_CONNECT;
        redisLog(REDIS_NOTICE,"SLAVE OF %s:%d enabled (user request)",
            server.masterhost, server.masterport);
    }

    addReply(c, shared.ok);
}
