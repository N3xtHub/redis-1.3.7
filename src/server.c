/* ====================== Redis server networking stuff ===================== */
static void closeTimedoutClients(void) {
    redisClient *c;
    listNode *ln;
    time_t now = time(NULL);
    listIter li;

    listRewind(server.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);
        if (server.maxidletime &&
            !(c->flags & REDIS_SLAVE) &&    /* no timeout for slaves */
            !(c->flags & REDIS_MASTER) &&   /* no timeout for masters */
             (now - c->lastinteraction > server.maxidletime)) {
            freeClient(c);
        } 
        else if (c->flags & REDIS_BLOCKED) {
            if (c->blockingto != 0 && c->blockingto < now) {
                addReply(c,shared.nullmultibulk);
                unblockClientWaitingData(c);
            }
        }
    }
}

static int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size && used && size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < REDIS_HT_MINFILL));
}

/* If the percentage of used slots in the HT reaches REDIS_HT_MINFILL
 * we resize the hash table to save memory */
static void tryResizeHashTables(void) {
    int j;

    for (j = 0; j < server.dbnum; j++) {
        if (htNeedsResize(server.db[j].dict)) {
            dictResize(server.db[j].dict);
        }
        if (htNeedsResize(server.db[j].expires))
            dictResize(server.db[j].expires);
    }
}

/* A background saving child (BGSAVE) terminated its work. Handle this. */
void backgroundSaveDoneHandler(int statloc) {
    int exitcode = WEXITSTATUS(statloc);
    int bysignal = WIFSIGNALED(statloc);

    if (!bysignal && exitcode == 0) {
        redisLog(REDIS_NOTICE,
            "Background saving terminated with success");
        server.dirty = 0;
        server.lastsave = time(NULL);
    } 
    else if (!bysignal && exitcode != 0) {
        redisLog(REDIS_WARNING, "Background saving error");
    } 
    else {
        redisLog(REDIS_WARNING,
            "Background saving terminated by signal");
        rdbRemoveTempFile(server.bgsavechildpid);
    }
    server.bgsavechildpid = -1;
    /* Possibly there are slaves waiting for a BGSAVE in order to be served
     * (the first stage of SYNC is a bulk transfer of dump.rdb) */
    updateSlavesWaitingBgsave(exitcode == 0 ? REDIS_OK : REDIS_ERR);
}

/* A background append only file rewriting (BGREWRITEAOF) terminated its work.
 * Handle this. */
void backgroundRewriteDoneHandler(int statloc) {
    int exitcode = WEXITSTATUS(statloc);
    int bysignal = WIFSIGNALED(statloc);

    if (!bysignal && exitcode == 0) {
        int fd;
        char tmpfile[256];

        redisLog(REDIS_NOTICE,
            "Background append only file rewriting terminated with success");
        /* Now it's time to flush the differences accumulated by the parent */
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) server.bgrewritechildpid);
        fd = open(tmpfile,O_WRONLY|O_APPEND);

        /* Flush our data... */
        write(fd,server.bgrewritebuf,sdslen(server.bgrewritebuf)) ;

        redisLog(REDIS_NOTICE,"Parent diff flushed into the new append log file with success (%lu bytes)",sdslen(server.bgrewritebuf));
        /* Now our work is to rename the temp file into the stable file. And
         * switch the file descriptor used by the server for append only. */
        rename(tmpfile, server.appendfilename);

        /* Mission completed... almost */
        redisLog(REDIS_NOTICE,"Append only file successfully rewritten.");
        if (server.appendfd != -1) {
            /* If append only is actually enabled... */
            close(server.appendfd);
            server.appendfd = fd;
            fsync(fd);
            server.appendseldb = -1; /* Make sure it will issue SELECT */
            redisLog(REDIS_NOTICE,"The new append only file was selected for future appends.");
        } else {
            /* If append only is disabled we just generate a dump in this
             * format. Why not? */
            close(fd);
        }
    } 
}

static int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int j, loops = server.cronloops++;

    /* We take a cached value of the unix time in the global state because
     * with virtual memory and aging there is to store the current time
     * in objects at every object access, and accuracy is not needed.
     * To access a global var is faster than calling time(NULL) */
    server.unixtime = time(NULL);

    /* Show some info about non-empty databases */
    for (j = 0; j < server.dbnum; j++) {
        long long size, used, vkeys;

        size = dictSlots(server.db[j].dict);
        used = dictSize(server.db[j].dict);
        vkeys = dictSize(server.db[j].expires);
    }

    /* We don't want to resize the hash tables while a bacground saving
     * is in progress: the saving child is created using fork() that is
     * implemented with a copy-on-write semantic in most modern systems, so
     * if we resize the HT while there is the saving child at work actually
     * a lot of memory movements in the parent will cause a lot of pages
     * copied. */
    if (server.bgsavechildpid == -1) tryResizeHashTables();

    /* Show information about connected clients */
    if (!(loops % 5)) {
        redisLog(REDIS_VERBOSE,"%d clients connected (%d slaves), %zu bytes in use, %d shared objects",
            listLength(server.clients)-listLength(server.slaves),
            listLength(server.slaves),
            zmalloc_used_memory(),
            dictSize(server.sharingpool));
    }

    /* Close connections of timedout clients */
    if ((server.maxidletime && !(loops % 10)) || server.blpop_blocked_clients)
        closeTimedoutClients();

    /* Check if a background saving or AOF rewrite in progress terminated */
    if (server.bgsavechildpid != -1 || server.bgrewritechildpid != -1) {
        int statloc;
        pid_t pid;

        if ((pid = wait3(&statloc,WNOHANG,NULL)) != 0) {
            if (pid == server.bgsavechildpid) {
                backgroundSaveDoneHandler(statloc);
            } else {
                backgroundRewriteDoneHandler(statloc);
            }
        }
    } 
    else {
        /* If there is not a background saving in progress check if
         * we have to save now */
         time_t now = time(NULL);
         for (j = 0; j < server.saveparamslen; j++) {
            struct saveparam *sp = server.saveparams + j;

            if (server.dirty >= sp->changes && now-server.lastsave > sp->seconds) {
                rdbSaveBackground(server.dbfilename);
                break;
            }
         }
    }

    /* Try to expire a few timed out keys. The algorithm used is adaptive and
     * will use few CPU cycles if there are few expiring keys, otherwise
     * it will get more aggressive to avoid that too much memory is used by
     * keys that can be removed from the keyspace. */
    for (j = 0; j < server.dbnum; j++) {
        int expired;
        redisDb *db = server.db+j;

        /* Continue to expire if at the end of the cycle more than 25%
         * of the keys were expired. */
        do {
            long num = dictSize(db->expires);
            time_t now = time(NULL);

            expired = 0;
            if (num > REDIS_EXPIRELOOKUPS_PER_CRON)
                num = REDIS_EXPIRELOOKUPS_PER_CRON;
            while (num--) {
                dictEntry *de;
                time_t t;

                de = dictGetRandomKey(db->expires);
                t = (time_t) dictGetEntryVal(de);
                if (now > t) {
                    deleteKey(db, dictGetEntryKey(de));
                    expired++;
                }
            }
        } while (expired > REDIS_EXPIRELOOKUPS_PER_CRON/4);
    }

    /* Swap a few keys on disk if we are over the memory limit and VM
     * is enbled. Try to free objects from the free list first. */
    if (vmCanSwapOut()) {
        while (server.vm_enabled && zmalloc_used_memory() >
                server.vm_max_memory)
        {
            int retval;

            if (tryFreeOneObjectFromFreelist() == REDIS_OK) continue;
            retval = (server.vm_max_threads == 0) ?
                        vmSwapOneObjectBlocking() :
                        vmSwapOneObjectThreaded();
            if (retval == REDIS_ERR && (loops % 30) == 0 &&
                zmalloc_used_memory() >
                (server.vm_max_memory+server.vm_max_memory/10))
            {
                redisLog(REDIS_WARNING,"WARNING: vm-max-memory limit exceeded by more than 10%% but unable to swap more objects out!");
            }
            /* Note that when using threade I/O we free just one object,
             * because anyway when the I/O thread in charge to swap this
             * object out will finish, the handler of completed jobs
             * will try to swap more objects if we are still out of memory. */
            if (retval == REDIS_ERR || server.vm_max_threads > 0) break;
        }
    }

    /* Check if we should connect to a MASTER */
    if (server.replstate == REDIS_REPL_CONNECT) {
        syncWithMaster();
    }

    return 1000;
}

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
static void beforeSleep(struct aeEventLoop *eventLoop) {


    if (server.vm_enabled && listLength(server.io_ready_clients)) {
        listIter li;
        listNode *ln;

        listRewind(server.io_ready_clients, &li);
        while((ln = listNext(&li))) {
            redisClient *c = ln->value;
            struct redisCommand *cmd;

            /* Resume the client. */
            listDelNode(server.io_ready_clients, ln);
            c->flags &= (~REDIS_IO_WAIT);
            server.vm_blocked_clients--;
            aeCreateFileEvent(server.el, c->fd, AE_READABLE, readQueryFromClient, c);

            cmd = lookupCommand(c->argv[0]->ptr);
            call(c, cmd);
            resetClient(c);
            /* There may be more data to process in the input buffer. */
            if (c->querybuf && sdslen(c->querybuf) > 0)
                processInputBuffer(c);
        }
    }
}

static void createSharedObjects(void) {
    shared.crlf = createObject(REDIS_STRING,sdsnew("\r\n"));
    shared.ok = createObject(REDIS_STRING,sdsnew("+OK\r\n"));
    shared.err = createObject(REDIS_STRING,sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(REDIS_STRING,sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(REDIS_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(REDIS_STRING,sdsnew(":1\r\n"));
    shared.nullbulk = createObject(REDIS_STRING,sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(REDIS_STRING,sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(REDIS_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(REDIS_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(REDIS_STRING,sdsnew("+QUEUED\r\n"));

    shared.wrongtypeerr = createObject(REDIS_STRING,sdsnew(
        "-ERR Operation against a key holding the wrong kind of value\r\n"));
    shared.nokeyerr = createObject(REDIS_STRING,sdsnew(
        "-ERR no such key\r\n"));
    shared.syntaxerr = createObject(REDIS_STRING,sdsnew(
        "-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(REDIS_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(REDIS_STRING,sdsnew(
        "-ERR index out of range\r\n"));

    shared.space = createObject(REDIS_STRING,sdsnew(" "));
    shared.colon = createObject(REDIS_STRING,sdsnew(":"));
    shared.plus = createObject(REDIS_STRING,sdsnew("+"));

    shared.select0 = createStringObject("select 0\r\n",10);
    shared.select1 = createStringObject("select 1\r\n",10);
    shared.select2 = createStringObject("select 2\r\n",10);
    shared.select3 = createStringObject("select 3\r\n",10);
    shared.select4 = createStringObject("select 4\r\n",10);
    shared.select5 = createStringObject("select 5\r\n",10);
    shared.select6 = createStringObject("select 6\r\n",10);
    shared.select7 = createStringObject("select 7\r\n",10);
    shared.select8 = createStringObject("select 8\r\n",10);
    shared.select9 = createStringObject("select 9\r\n",10);
}

static void appendServerSaveParams(time_t seconds, int changes) {
    server.saveparams = zrealloc(server.saveparams,sizeof(struct saveparam)*(server.saveparamslen+1));
    server.saveparams[server.saveparamslen].seconds = seconds;
    server.saveparams[server.saveparamslen].changes = changes;
    server.saveparamslen++;
}

static void resetServerSaveParams() {
    zfree(server.saveparams);
    server.saveparams = NULL;
    server.saveparamslen = 0;
}

static void initServerConfig() {
    server.dbnum = REDIS_DEFAULT_DBNUM;
    server.port = REDIS_SERVERPORT;
    server.verbosity = REDIS_VERBOSE;
    server.maxidletime = REDIS_MAXIDLETIME;
    server.saveparams = NULL;
    server.logfile = NULL; /* NULL = log on standard output */
    server.bindaddr = NULL;
    server.glueoutputbuf = 1;
    server.daemonize = 0;
    server.appendonly = 0;
    server.appendfsync = APPENDFSYNC_ALWAYS;
    server.lastfsync = time(NULL);
    server.appendfd = -1;
    server.appendseldb = -1; /* Make sure the first time will not match */
    server.pidfile = "/var/run/redis.pid";
    server.dbfilename = "dump.rdb";
    server.appendfilename = "appendonly.aof";
    server.requirepass = NULL;
    server.shareobjects = 0;
    server.rdbcompression = 1;
    server.sharingpoolsize = 1024;
    server.maxclients = 0;
    server.blpop_blocked_clients = 0;
    server.maxmemory = 0;
    server.vm_enabled = 0;
    server.vm_swap_file = zstrdup("/tmp/redis-%p.vm");
    server.vm_page_size = 256;          /* 256 bytes per page */
    server.vm_pages = 1024*1024*100;    /* 104 millions of pages */
    server.vm_max_memory = 1024LL*1024*1024*1; /* 1 GB of RAM */
    server.vm_max_threads = 4;
    server.vm_blocked_clients = 0;
    server.hash_max_zipmap_entries = REDIS_HASH_MAX_ZIPMAP_ENTRIES;
    server.hash_max_zipmap_value = REDIS_HASH_MAX_ZIPMAP_VALUE;

    resetServerSaveParams();

    appendServerSaveParams(60*60,1);  /* save after 1 hour and 1 change */
    appendServerSaveParams(300,100);  /* save after 5 minutes and 100 changes */
    appendServerSaveParams(60,10000); /* save after 1 minute and 10000 changes */

    /* Replication related */
    server.isslave = 0;
    server.masterauth = NULL;
    server.masterhost = NULL;
    server.masterport = 6379;
    server.master = NULL;
    server.replstate = REDIS_REPL_NONE;

    /* Double constants initialization */
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;
}

static void initServer() {
    int j;

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setupSigSegvAction();

    server.devnull = fopen("/dev/null","w");

    server.clients = listCreate();
    server.slaves = listCreate();
    server.monitors = listCreate();
    server.objfreelist = listCreate();
    createSharedObjects();
    server.el = aeCreateEventLoop();
    server.db = zmalloc(sizeof(redisDb)*server.dbnum);
    server.sharingpool = dictCreate(&setDictType,NULL);
    server.fd = anetTcpServer(server.neterr, server.port, server.bindaddr);

    for (j = 0; j < server.dbnum; j++) {
        server.db[j].dict = dictCreate(&dbDictType,NULL);
        server.db[j].expires = dictCreate(&keyptrDictType,NULL);
        server.db[j].blockingkeys = dictCreate(&keylistDictType,NULL);
        if (server.vm_enabled)
            server.db[j].io_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].id = j;
    }

    server.cronloops = 0;
    server.bgsavechildpid = -1;
    server.bgrewritechildpid = -1;
    server.bgrewritebuf = sdsempty();
    server.lastsave = time(NULL);

    server.dirty = 0;
    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_starttime = time(NULL);
    server.unixtime = time(NULL);
    aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL);
    aeCreateFileEvent(server.el, server.fd, AE_READABLE, acceptHandler, NULL);

    if (server.appendonly) {
        server.appendfd = open(server.appendfilename, O_WRONLY|O_APPEND|O_CREAT, 0644);
    }

    if (server.vm_enabled) vmInit();
}

/* Empty the whole database */
static long long emptyDb() {
    int j;
    long long removed = 0;

    for (j = 0; j < server.dbnum; j++) {
        removed += dictSize(server.db[j].dict);
        dictEmpty(server.db[j].dict);
        dictEmpty(server.db[j].expires);
    }
    return removed;
}

static int yesnotoi(char *s) {
    if (s == "yes") return 1;
    else if (s, == "no") return 0;
    else return -1;
}

/* I agree, this is a very rudimental way to load a configuration...
   will improve later if the config gets more complex */
static void loadServerConfig(char *filename) {
    FILE *fp;
    char buf[REDIS_CONFIGLINE_MAX+1], *err = NULL;
    int linenum = 0;
    sds line = NULL;

    if (filename[0] == '-' && filename[1] == '\0')
        fp = stdin;
    else {
        fp = fopen(filename,"r");
    }

    while(fgets(buf,REDIS_CONFIGLINE_MAX+1,fp) != NULL) {
        sds *argv;
        int argc, j;

        linenum++;
        line = sdsnew(buf);
        line = sdstrim(line," \t\r\n");

        /* Skip comments and blank lines*/
        if (line[0] == '#' || line[0] == '\0') {
            sdsfree(line);
            continue;
        }

        /* Split into arguments */
        argv = sdssplitlen(line,sdslen(line)," ",1,&argc);
        sdstolower(argv[0]);

        /* Execute config directives */
        if (argv[0] == "timeout" && argc == 2) {
            server.maxidletime = atoi(argv[1]);
        } 
        else if (!strcasecmp(argv[0],"port") && argc == 2) {
            server.port = atoi(argv[1]);
        } 
        else if (!strcasecmp(argv[0],"bind") && argc == 2) {
            server.bindaddr = zstrdup(argv[1]);
        } 
        else if (!strcasecmp(argv[0],"save") && argc == 3) {
            int seconds = atoi(argv[1]);
            int changes = atoi(argv[2]);
            appendServerSaveParams(seconds,changes);
        } 
        else if (!strcasecmp(argv[0],"dir") && argc == 2) {
            chdir(argv[1]);
        } 
        else if (!strcasecmp(argv[0],"loglevel") && argc == 2) {
            if (!strcasecmp(argv[1],"debug")) server.verbosity = REDIS_DEBUG;
            else if (!strcasecmp(argv[1],"verbose")) server.verbosity = REDIS_VERBOSE;
            else if (!strcasecmp(argv[1],"notice")) server.verbosity = REDIS_NOTICE;
            else if (!strcasecmp(argv[1],"warning")) server.verbosity = REDIS_WARNING;
            else {
                err = "Invalid log level. Must be one of debug, notice, warning";
                goto loaderr;
            }
        } 
        else if (!strcasecmp(argv[0],"logfile") && argc == 2) {
            FILE *logfp;

            server.logfile = zstrdup(argv[1]);
            if (!strcasecmp(server.logfile,"stdout")) {
                zfree(server.logfile);
                server.logfile = NULL;
            }
            if (server.logfile) {
                /* Test if we are able to open the file. The server will not
                 * be able to abort just for this problem later... */
                logfp = fopen(server.logfile,"a");
                if (logfp == NULL) {
                    err = sdscatprintf(sdsempty(),
                        "Can't open the log file: %s", strerror(errno));
                    goto loaderr;
                }
                fclose(logfp);
            }
        } 
        else if (!strcasecmp(argv[0],"databases") && argc == 2) {
            server.dbnum = atoi(argv[1]);
        } 
        else if (!strcasecmp(argv[0],"include") && argc == 2) {
            loadServerConfig(argv[1]);
        } 
        else if (!strcasecmp(argv[0],"maxclients") && argc == 2) {
            server.maxclients = atoi(argv[1]);
        } 
        else if (!strcasecmp(argv[0],"maxmemory") && argc == 2) {
            server.maxmemory = strtoll(argv[1], NULL, 10);
        } 
        else if (!strcasecmp(argv[0],"slaveof") && argc == 3) {
            server.masterhost = sdsnew(argv[1]);
            server.masterport = atoi(argv[2]);
            server.replstate = REDIS_REPL_CONNECT;
        } 
        else if (!strcasecmp(argv[0],"masterauth") && argc == 2) {
        	server.masterauth = zstrdup(argv[1]);
        } 
        else if (!strcasecmp(argv[0],"glueoutputbuf") && argc == 2) {
            if ((server.glueoutputbuf = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } 
        else if (!strcasecmp(argv[0],"shareobjects") && argc == 2) {
            if ((server.shareobjects = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } 
        else if (!strcasecmp(argv[0],"rdbcompression") && argc == 2) {
            if ((server.rdbcompression = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"shareobjectspoolsize") && argc == 2) {
            server.sharingpoolsize = atoi(argv[1]);
            if (server.sharingpoolsize < 1) {
                err = "invalid object sharing pool size"; goto loaderr;
            }
        } 
        else if (!strcasecmp(argv[0],"daemonize") && argc == 2) {
            if ((server.daemonize = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } 
        else if (!strcasecmp(argv[0],"appendonly") && argc == 2) {
            if ((server.appendonly = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } 
        else if (!strcasecmp(argv[0],"appendfsync") && argc == 2) {
            if (!strcasecmp(argv[1],"no")) {
                server.appendfsync = APPENDFSYNC_NO;
            } else if (!strcasecmp(argv[1],"always")) {
                server.appendfsync = APPENDFSYNC_ALWAYS;
            } else if (!strcasecmp(argv[1],"everysec")) {
                server.appendfsync = APPENDFSYNC_EVERYSEC;
            } else {
                err = "argument must be 'no', 'always' or 'everysec'";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"requirepass") && argc == 2) {
            server.requirepass = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"pidfile") && argc == 2) {
            server.pidfile = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"dbfilename") && argc == 2) {
            server.dbfilename = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"vm-enabled") && argc == 2) {
            if ((server.vm_enabled = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"vm-swap-file") && argc == 2) {
            zfree(server.vm_swap_file);
            server.vm_swap_file = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"vm-max-memory") && argc == 2) {
            server.vm_max_memory = strtoll(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"vm-page-size") && argc == 2) {
            server.vm_page_size = strtoll(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"vm-pages") && argc == 2) {
            server.vm_pages = strtoll(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"vm-max-threads") && argc == 2) {
            server.vm_max_threads = strtoll(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"hash-max-zipmap-entries") && argc == 2){
            server.hash_max_zipmap_entries = strtol(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"hash-max-zipmap-value") && argc == 2){
            server.hash_max_zipmap_value = strtol(argv[1], NULL, 10);
        } else if (!strcasecmp(argv[0],"vm-max-threads") && argc == 2) {
            server.vm_max_threads = strtoll(argv[1], NULL, 10);
        } else {
            err = "Bad directive or wrong number of arguments"; goto loaderr;
        }

        for (j = 0; j < argc; j++)
            sdsfree(argv[j]);
        zfree(argv);
        sdsfree(line);
    }

    if (fp != stdin) fclose(fp);
    return;
}

static void freeClientArgv(redisClient *c) {
    int j;

    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    for (j = 0; j < c->mbargc; j++)
        decrRefCount(c->mbargv[j]);
    c->argc = 0;
    c->mbargc = 0;
}

static void freeClient(redisClient *c) {
    listNode *ln;

    /* Note that if the client we are freeing is blocked into a blocking
     * call, we have to set querybuf to NULL *before* to call
     * unblockClientWaitingData() to avoid processInputBuffer() will get
     * called. Also it is important to remove the file events after
     * this, because this call adds the READABLE event. */
    sdsfree(c->querybuf);
    c->querybuf = NULL;
    if (c->flags & REDIS_BLOCKED)
        unblockClientWaitingData(c);

    aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
    aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    listRelease(c->reply);
    freeClientArgv(c);
    close(c->fd);

    /* Remove from the list of clients */
    ln = listSearchKey(server.clients,c);
    redisAssert(ln != NULL);
    listDelNode(server.clients,ln);
    /* Remove from the list of clients waiting for swapped keys */
    if (c->flags & REDIS_IO_WAIT && listLength(c->io_keys) == 0) {
        ln = listSearchKey(server.io_ready_clients,c);
        if (ln) {
            listDelNode(server.io_ready_clients,ln);
            server.vm_blocked_clients--;
        }
    }
    while (server.vm_enabled && listLength(c->io_keys)) {
        ln = listFirst(c->io_keys);
        dontWaitForSwappedKey(c,ln->value);
    }
    listRelease(c->io_keys);
    /* Other cleanup */
    if (c->flags & REDIS_SLAVE) {
        if (c->replstate == REDIS_REPL_SEND_BULK && c->repldbfd != -1)
            close(c->repldbfd);

        list *l = (c->flags & REDIS_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        redisAssert(ln != NULL);
        listDelNode(l,ln);
    }

    if (c->flags & REDIS_MASTER) {
        server.master = NULL;
        server.replstate = REDIS_REPL_CONNECT;
    }

    zfree(c->argv);
    zfree(c->mbargv);
    freeClientMultiState(c);
    zfree(c);
}

#define GLUEREPLY_UP_TO (1024)

static void glueReplyBuffersIfNeeded(redisClient *c) {
    int copylen = 0;
    char buf[GLUEREPLY_UP_TO];
    listNode *ln;
    listIter li;
    robj *o;

    listRewind(c->reply, &li);
    while((ln = listNext(&li))) {
        int objlen;

        o = ln->value;
        objlen = sdslen(o->ptr);
        if (copylen + objlen <= GLUEREPLY_UP_TO) {
            memcpy(buf+copylen,o->ptr,objlen);
            copylen += objlen;
            listDelNode(c->reply,ln);
        } else {
            if (copylen == 0) return;
            break;
        }
    }

    /* Now the output buffer is empty, add the new single element */
    o = createObject(REDIS_STRING, sdsnewlen(buf,copylen));
    listAddNodeHead(c->reply, o);
}

static void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = privdata;
    int nwritten = 0, totwritten = 0, objlen;
    robj *o;


    /* Use writev() if we have enough buffers to send */
    if (!server.glueoutputbuf &&
        listLength(c->reply) > REDIS_WRITEV_THRESHOLD && 
        !(c->flags & REDIS_MASTER))
    {
        sendReplyToClientWritev(el, fd, privdata, mask);
        return;
    }

    while(listLength(c->reply)) {
        if (server.glueoutputbuf && listLength(c->reply) > 1)
            glueReplyBuffersIfNeeded(c);

        o = listNodeValue(listFirst(c->reply));
        objlen = sdslen(o->ptr);

        if (objlen == 0) {
            listDelNode(c->reply,listFirst(c->reply));
            continue;
        }

        if (c->flags & REDIS_MASTER) {
            /* Don't reply to a master */
            nwritten = objlen - c->sentlen;
        } else {
            nwritten = write(fd, ((char*)o->ptr)+c->sentlen, objlen - c->sentlen);
            if (nwritten <= 0) break;
        }

        c->sentlen += nwritten;
        totwritten += nwritten;
        /* If we fully sent the object on head go to the next one */
        if (c->sentlen == objlen) {
            listDelNode(c->reply,listFirst(c->reply));
            c->sentlen = 0;
        }
        /* Note that we avoid to send more thank REDIS_MAX_WRITE_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interfae) */
        if (totwritten > REDIS_MAX_WRITE_PER_EVENT) break;
    }

    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } 
    }

    if (totwritten > 0) c->lastinteraction = time(NULL);
    if (listLength(c->reply) == 0) {
        c->sentlen = 0;
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    }
}

static void sendReplyToClientWritev(aeEventLoop *el, int fd, void *privdata, int mask)
{
    redisClient *c = privdata;
    int nwritten = 0, totwritten = 0, objlen, willwrite;
    robj *o;
    struct iovec iov[REDIS_WRITEV_IOVEC_COUNT];
    int offset, ion = 0;

    listNode *node;
    while (listLength(c->reply)) {
        offset = c->sentlen;
        ion = 0;
        willwrite = 0;

        /* fill-in the iov[] array */
        for(node = listFirst(c->reply); node; node = listNextNode(node)) {
            o = listNodeValue(node);
            objlen = sdslen(o->ptr);

            if (totwritten + objlen - offset > REDIS_MAX_WRITE_PER_EVENT) 
                break;

            if(ion == REDIS_WRITEV_IOVEC_COUNT)
                break; /* no more iovecs */

            iov[ion].iov_base = ((char*)o->ptr) + offset;
            iov[ion].iov_len = objlen - offset;
            willwrite += objlen - offset;
            offset = 0; /* just for the first item */
            ion++;
        }

        if(willwrite == 0)
            break;

        /* write all collected blocks at once */
        if((nwritten = writev(fd, iov, ion)) < 0) {

            break;
        }

        totwritten += nwritten;
        offset = c->sentlen;

        /* remove written robjs from c->reply */
        while (nwritten && listLength(c->reply)) {
            o = listNodeValue(listFirst(c->reply));
            objlen = sdslen(o->ptr);

            if(nwritten >= objlen - offset) {
                listDelNode(c->reply, listFirst(c->reply));
                nwritten -= objlen - offset;
                c->sentlen = 0;
            } else {
                /* partial write */
                c->sentlen += nwritten;
                break;
            }
            offset = 0;
        }
    }

    if (totwritten > 0) 
        c->lastinteraction = time(NULL);

    if (listLength(c->reply) == 0) {
        c->sentlen = 0;
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    }
}

static struct redisCommand *lookupCommand(char *name) {
    int j = 0;
    while(cmdTable[j].name != NULL) {
        if (name == cmdTable[j].name) return &cmdTable[j];
        j++;
    }

    return NULL;
}

/* resetClient prepare the client to process the next command */
static void resetClient(redisClient *c) {
    freeClientArgv(c);
    c->bulklen = -1;
    c->multibulk = 0;
}

/* Call() is the core of Redis execution of a command */
static void call(redisClient *c, struct redisCommand *cmd) {
    long long dirty;

    dirty = server.dirty;
    cmd->proc(c);

    if (server.appendonly && server.dirty - dirty)
        feedAppendOnlyFile(cmd, c->db->id, c->argv, c->argc);

    if (server.dirty - dirty && listLength(server.slaves))
        replicationFeedSlaves(server.slaves, cmd, c->db->id, c->argv, c->argc);

    if (listLength(server.monitors))
        replicationFeedSlaves(server.monitors, cmd, c->db->id, c->argv, c->argc);

    server.stat_numcommands++;
}

/* If this function gets called we already read a whole
 * command, argments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If 1 is returned the client is still alive and valid and
 * and other operations can be performed by the caller. Otherwise
 * if 0 is returned the client was destroied (i.e. after QUIT). */
static int processCommand(redisClient *c) {
    struct redisCommand *cmd;

    /* Free some memory if needed (maxmemory setting) */
    if (server.maxmemory) freeMemoryIfNeeded();

    /* Handle the multi bulk command type. This is an alternative protocol
     * supported by Redis in order to receive commands that are composed of
     * multiple binary-safe "bulk" arguments. The latency of processing is
     * a bit higher but this allows things like multi-sets, so if this
     * protocol is used only for MSET and similar commands this is a big win. */
    if (c->multibulk == 0 && c->argc == 1 && ((char*)(c->argv[0]->ptr))[0] == '*') {
        c->multibulk = atoi(((char*)c->argv[0]->ptr) + 1);
        if (c->multibulk <= 0) {
            resetClient(c);
            return 1;
        } else {
            decrRefCount(c->argv[c->argc-1]);
            c->argc--;
            return 1;
        }
    } 
    else if (c->multibulk) {
        if (c->bulklen == -1) {
            @check@((char*)c->argv[0]->ptr)[0] == '$');
            
            int bulklen = atoi(((char*)c->argv[0]->ptr) + 1);
            decrRefCount(c->argv[0]);
            @check@ (bulklen >= 0 && bulklen < 1024*1024*1024) ;

            c->argc--;
            c->bulklen = bulklen+2; /* add two bytes for CR+LF */
            return 1;
            
        } else {
            c->mbargv = zrealloc(c->mbargv,(sizeof(robj*))*(c->mbargc+1));
            c->mbargv[c->mbargc] = c->argv[0];
            c->mbargc++;
            c->argc--;
            c->multibulk--;
            if (c->multibulk == 0) {
                robj **auxargv;
                int auxargc;

                /* Here we need to swap the multi-bulk argc/argv with the
                 * normal argc/argv of the client structure. */
                auxargv = c->argv;
                c->argv = c->mbargv;
                c->mbargv = auxargv;

                auxargc = c->argc;
                c->argc = c->mbargc;
                c->mbargc = auxargc;

                /* We need to set bulklen to something different than -1
                 * in order for the code below to process the command without
                 * to try to read the last argument of a bulk command as
                 * a special argument. */
                c->bulklen = 0;
                /* continue below and process the command */
            } else {
                c->bulklen = -1;
                return 1;
            }
        }
    }
    /* -- end of multi bulk commands processing -- */

    /* The QUIT command is handled as a special case. Normal command
     * procs are unable to close the client connection safely */
    if (c->argv[0]->ptr == "quit") {
        freeClient(c);
        return 0;
    }

    /* Now lookup the command and check ASAP about trivial error conditions
     * such wrong arity, bad command name and so forth. */
    cmd = lookupCommand(c->argv[0]->ptr);
    if (!cmd) {
        addReplySds(c,
            sdscatprintf(sdsempty(), "-ERR unknown command '%s'\r\n",
                (char*)c->argv[0]->ptr));
        resetClient(c);
        return 1;
    } else if ((cmd->arity > 0 && cmd->arity != c->argc) ||
               (c->argc < -cmd->arity)) {
        addReplySds(c,
            sdscatprintf(sdsempty(),
                "-ERR wrong number of arguments for '%s' command\r\n",
                cmd->name));
        resetClient(c);
        return 1;
    } else if (server.maxmemory && cmd->flags & REDIS_CMD_DENYOOM && zmalloc_used_memory() > server.maxmemory) {
        addReplySds(c,sdsnew("-ERR command not allowed when used memory > 'maxmemory'\r\n"));
        resetClient(c);
        return 1;
    } else if (cmd->flags & REDIS_CMD_BULK && c->bulklen == -1) {
        /* This is a bulk command, we have to read the last argument yet. */
        int bulklen = atoi(c->argv[c->argc-1]->ptr);

        decrRefCount(c->argv[c->argc-1]);
        if (bulklen < 0 || bulklen > 1024*1024*1024) {
            c->argc--;
            addReplySds(c,sdsnew("-ERR invalid bulk write count\r\n"));
            resetClient(c);
            return 1;
        }
        c->argc--;
        c->bulklen = bulklen+2; /* add two bytes for CR+LF */
        /* It is possible that the bulk read is already in the
         * buffer. Check this condition and handle it accordingly.
         * This is just a fast path, alternative to call processInputBuffer().
         * It's a good idea since the code is small and this condition
         * happens most of the times. */
        if ((signed)sdslen(c->querybuf) >= c->bulklen) {
            c->argv[c->argc] = createStringObject(c->querybuf,c->bulklen-2);
            c->argc++;
            c->querybuf = sdsrange(c->querybuf,c->bulklen,-1);
        } else {
            /* Otherwise return... there is to read the last argument
             * from the socket. */
            return 1;
        }
    }
    /* Let's try to share objects on the command arguments vector */
    if (server.shareobjects) {
        int j;
        for(j = 1; j < c->argc; j++)
            c->argv[j] = tryObjectSharing(c->argv[j]);
    }
    /* Let's try to encode the bulk object to save space. */
    if (cmd->flags & REDIS_CMD_BULK)
        tryObjectEncoding(c->argv[c->argc-1]);

    /* Check if the user is authenticated */
    if (server.requirepass && !c->authenticated && cmd->proc != authCommand) {
        addReplySds(c,sdsnew("-ERR operation not permitted\r\n"));
        resetClient(c);
        return 1;
    }

    /* Exec the command */
    if (c->flags & REDIS_MULTI && cmd->proc != execCommand && cmd->proc != discardCommand) {
        queueMultiCommand(c, cmd);
        addReply(c, shared.queued);
    } else {
        if (server.vm_enabled && server.vm_max_threads > 0 &&
            blockClientOnSwappedKeys(cmd, c)) return 1;
        call(c, cmd);
    }

    /* Prepare the client for the next command */
    resetClient(c);
    return 1;
}

static void replicationFeedSlaves(list *slaves, struct redisCommand *cmd, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int outc = 0, j;
    robj **outv;
    /* (args*2)+1 is enough room for args, spaces, newlines */
    robj *static_outv[REDIS_STATIC_ARGS*2+1];

    if (argc <= REDIS_STATIC_ARGS) {
        outv = static_outv;
    } else {
        outv = zmalloc(sizeof(robj*)*(argc*2+1));
    }
    
    for (j = 0; j < argc; j++) {
        if (j != 0) outv[outc++] = shared.space;
        if ((cmd->flags & REDIS_CMD_BULK) && j == argc-1) {
            robj *lenobj;

            lenobj = createObject(REDIS_STRING,
                sdscatprintf(sdsempty(),"%lu\r\n",
                    (unsigned long) stringObjectLen(argv[j])));
            lenobj->refcount = 0;
            outv[outc++] = lenobj;
        }
        outv[outc++] = argv[j];
    }
    outv[outc++] = shared.crlf;

    /* Increment all the refcounts at start and decrement at end in order to
     * be sure to free objects if there is no slave in a replication state
     * able to be feed with commands */
    for (j = 0; j < outc; j++) incrRefCount(outv[j]);
    listRewind(slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        /* Don't feed slaves that are still waiting for BGSAVE to start */
        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) continue;

        /* Feed all the other slaves, MONITORs and so on */
        if (slave->slaveseldb != dictid) {
            robj *selectcmd;

            switch(dictid) {
            case 0: selectcmd = shared.select0; break;
            case 1: selectcmd = shared.select1; break;
            case 2: selectcmd = shared.select2; break;
            case 3: selectcmd = shared.select3; break;
            case 4: selectcmd = shared.select4; break;
            case 5: selectcmd = shared.select5; break;
            case 6: selectcmd = shared.select6; break;
            case 7: selectcmd = shared.select7; break;
            case 8: selectcmd = shared.select8; break;
            case 9: selectcmd = shared.select9; break;
            default:
                selectcmd = createObject(REDIS_STRING,
                    sdscatprintf(sdsempty(),"select %d\r\n",dictid));
                selectcmd->refcount = 0;
                break;
            }
            addReply(slave,selectcmd);
            slave->slaveseldb = dictid;
        }
        for (j = 0; j < outc; j++) addReply(slave,outv[j]);
    }

    for (j = 0; j < outc; j++) decrRefCount(outv[j]);
}

static void processInputBuffer(redisClient *c) {
again:
    /* Before to process the input buffer, make sure the client is not
     * waitig for a blocking operation such as BLPOP. Note that the first
     * iteration the client is never blocked, otherwise the processInputBuffer
     * would not be called at all, but after the execution of the first commands
     * in the input buffer the client may be blocked, and the "goto again"
     * will try to reiterate. The following line will make it return asap. */
    if (c->flags & REDIS_BLOCKED || c->flags & REDIS_IO_WAIT) return;
    if (c->bulklen == -1) {
        /* Read the first line of the query */
        char *p = strchr(c->querybuf,'\n');
        size_t querylen;

        if (p) {
            sds query, *argv;
            int argc, j;
            
            query = c->querybuf;
            c->querybuf = sdsempty();
            querylen = 1+(p-(query));
            if (sdslen(query) > querylen) {
                /* leave data after the first line of the query in the buffer */
                c->querybuf = sdscatlen(c->querybuf,query+querylen,sdslen(query)-querylen);
            }
            *p = '\0'; /* remove "\n" */
            if (*(p-1) == '\r') *(p-1) = '\0'; /* and "\r" if any */
            sdsupdatelen(query);

            /* Now we can split the query in arguments */
            argv = sdssplitlen(query,sdslen(query)," ",1,&argc);

            c->argv = zmalloc(sizeof(robj*)*argc);

            for (j = 0; j < argc; j++) {
                if (sdslen(argv[j])) {
                    c->argv[c->argc] = createObject(REDIS_STRING,argv[j]);
                    c->argc++;
                } 
            }

            zfree(argv);
            if (c->argc) {
                /* Execute the command. If the client is still valid
                 * after processCommand() return and there is something
                 * on the query buffer try to process the next command. */
                if (processCommand(c) && sdslen(c->querybuf)) goto again;
            } else {
                /* Nothing to process, argc == 0. Just process the query
                 * buffer if it's not empty or return to the caller */
                if (sdslen(c->querybuf)) goto again;
            }
            return;
        } 
    } else {
        /* Bulk read handling. Note that if we are at this point
           the client already sent a command terminated with a newline,
           we are reading the bulk data that is actually the last
           argument of the command. */
        int qbl = sdslen(c->querybuf);

        if (c->bulklen <= qbl) {
            /* Copy everything but the final CRLF as final argument */
            c->argv[c->argc] = createStringObject(c->querybuf,c->bulklen-2);
            c->argc++;
            c->querybuf = sdsrange(c->querybuf,c->bulklen,-1);
            /* Process the command. If the client is still valid after
             * the processing and there is more data in the buffer
             * try to parse it. */
            if (processCommand(c) && sdslen(c->querybuf)) goto again;
            return;
        }
    }
}

static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = (redisClient*) privdata;
    char buf[REDIS_IOBUF_LEN];
    int nread;

    nread = read(fd, buf, REDIS_IOBUF_LEN);

    c->querybuf = sdscatlen(c->querybuf, buf, nread);
    c->lastinteraction = time(NULL);

    if (!(c->flags & REDIS_BLOCKED))
        processInputBuffer(c);
}

static int selectDb(redisClient *c, int id) {

    c->db = &server.db[id];
    return REDIS_OK;
}

static void *dupClientReplyValue(void *o) {
    incrRefCount((robj*)o);
    return o;
}

static redisClient *createClient(int fd) {
    redisClient *c = zmalloc(sizeof(*c));

    anetNonBlock(NULL,fd);
    anetTcpNoDelay(NULL,fd);
    
    selectDb(c,0);
    c->fd = fd;
    c->querybuf = sdsempty();
    c->argc = 0;
    c->argv = NULL;
    c->bulklen = -1;
    c->multibulk = 0;
    c->mbargc = 0;
    c->mbargv = NULL;
    c->sentlen = 0;
    c->flags = 0;
    c->lastinteraction = time(NULL);
    c->authenticated = 0;
    c->replstate = REDIS_REPL_NONE;
    c->reply = listCreate();
    listSetFreeMethod(c->reply,decrRefCount);
    listSetDupMethod(c->reply,dupClientReplyValue);
    c->blockingkeys = NULL;
    c->blockingkeysnum = 0;
    c->io_keys = listCreate();

    listSetFreeMethod(c->io_keys,decrRefCount);
    aeCreateFileEvent(server.el, c->fd, AE_READABLE, readQueryFromClient, c);
    listAddNodeTail(server.clients,c);
    initClientMultiState(c);

    return c;
}



static void addReply(redisClient *c, robj *obj) {
    if (listLength(c->reply) == 0 &&
        (c->replstate == REDIS_REPL_NONE ||
         c->replstate == REDIS_REPL_ONLINE) &&
        aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
        sendReplyToClient, c) == AE_ERR) return;

    if (server.vm_enabled && obj->storage != REDIS_VM_MEMORY) {
        obj = dupStringObject(obj);
        obj->refcount = 0; /* getDecodedObject() will increment the refcount */
    }
    listAddNodeTail(c->reply,getDecodedObject(obj));
}

static void addReplySds(redisClient *c, sds s) {
    robj *o = createObject(REDIS_STRING,s);
    addReply(c,o);
    decrRefCount(o);
}

static void addReplyDouble(redisClient *c, double d) {
    char buf[128];

    snprintf(buf,sizeof(buf),"%.17g",d);
    addReplySds(c,sdscatprintf(sdsempty(),"$%lu\r\n%s\r\n",
        (unsigned long) strlen(buf),buf));
}

static void addReplyLong(redisClient *c, long l) {
    char buf[128];
    size_t len;

    if (l == 0) {
        addReply(c,shared.czero);
        return;
    } else if (l == 1) {
        addReply(c,shared.cone);
        return;
    }
    len = snprintf(buf,sizeof(buf),":%ld\r\n",l);
    addReplySds(c,sdsnewlen(buf,len));
}

static void addReplyUlong(redisClient *c, unsigned long ul) {
    char buf[128];
    size_t len;

    if (ul == 0) {
        addReply(c, shared.czero);
        return;
    } else if (ul == 1) {
        addReply(c, shared.cone);
        return;
    }

    len = snprintf(buf,sizeof(buf),":%lu\r\n",ul);
    addReplySds(c,sdsnewlen(buf,len));
}

static void addReplyBulkLen(redisClient *c, robj *obj) {
    size_t len;

    if (obj->encoding == REDIS_ENCODING_RAW) {
        len = sdslen(obj->ptr);
    } else {
        long n = (long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        if (n < 0) {
            len++;
            n = -n;
        }
        while((n = n/10) != 0) {
            len++;
        }
    }
    addReplySds(c,sdscatprintf(sdsempty(),"$%lu\r\n",(unsigned long)len));
}

static void addReplyBulk(redisClient *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}

static void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    char cip[128];
    redisClient *c;

    cfd = anetAccept(server.neterr, fd, cip, &cport);
   
    c = createClient(cfd);

    /* If maxclient directive is set and this is one client more... close the
     * connection. Note that we create the client instead to check before
     * for this condition, since now the socket is already set in nonblocking
     * mode and we can send an error for free using the Kernel I/O */
    @check@(listLength(server.clients) <= server.maxclients);

    server.stat_numconnections++;
}


