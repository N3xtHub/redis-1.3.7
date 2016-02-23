// commands.c

/*================================== Commands =============================== */

static void authCommand(redisClient *c) {
    if (!server.requirepass || !strcmp(c->argv[1]->ptr, server.requirepass)) {
      c->authenticated = 1;
      addReply(c,shared.ok);
    } else {
      c->authenticated = 0;
      addReplySds(c,sdscatprintf(sdsempty(),"-ERR invalid password\r\n"));
    }
}

static void pingCommand(redisClient *c) {
    addReply(c,shared.pong);
}

static void echoCommand(redisClient *c) {
    addReplyBulk(c,c->argv[1]);
}

/*=================================== Strings =============================== */

static void setGenericCommand(redisClient *c, int nx) {
    int retval;

    if (nx) deleteIfVolatile(c->db,c->argv[1]);
    retval = dictAdd(c->db->dict,c->argv[1],c->argv[2]);
    if (retval == DICT_ERR) {
        if (!nx) {
            /* If the key is about a swapped value, we want a new key object
             * to overwrite the old. So we delete the old key in the database.
             * This will also make sure that swap pages about the old object
             * will be marked as free. */
            if (server.vm_enabled && deleteIfSwapped(c->db,c->argv[1]))
                incrRefCount(c->argv[1]);
            dictReplace(c->db->dict,c->argv[1],c->argv[2]);
            incrRefCount(c->argv[2]);
        } else {
            addReply(c,shared.czero);
            return;
        }
    } else {
        incrRefCount(c->argv[1]);
        incrRefCount(c->argv[2]);
    }
    server.dirty++;
    removeExpire(c->db,c->argv[1]);
    addReply(c, nx ? shared.cone : shared.ok);
}

static void setCommand(redisClient *c) {
    setGenericCommand(c,0);
}

static void setnxCommand(redisClient *c) {
    setGenericCommand(c,1);
}

static int getGenericCommand(redisClient *c) {
    robj *o;
    
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL)
        return REDIS_OK;

    if (o->type != REDIS_STRING) {
        addReply(c,shared.wrongtypeerr);
        return REDIS_ERR;
    } else {
        addReplyBulk(c,o);
        return REDIS_OK;
    }
}

static void getCommand(redisClient *c) {
    getGenericCommand(c);
}

static void getsetCommand(redisClient *c) {
    if (getGenericCommand(c) == REDIS_ERR) return;
    if (dictAdd(c->db->dict,c->argv[1],c->argv[2]) == DICT_ERR) {
        dictReplace(c->db->dict,c->argv[1],c->argv[2]);
    } else {
        incrRefCount(c->argv[1]);
    }
    incrRefCount(c->argv[2]);
    server.dirty++;
    removeExpire(c->db,c->argv[1]);
}

static void mgetCommand(redisClient *c) {
    int j;
  
    addReplySds(c,sdscatprintf(sdsempty(),"*%d\r\n",c->argc-1));
    for (j = 1; j < c->argc; j++) {
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) {
            addReply(c,shared.nullbulk);
        } else {
            if (o->type != REDIS_STRING) {
                addReply(c,shared.nullbulk);
            } else {
                addReplyBulk(c,o);
            }
        }
    }
}

static void msetGenericCommand(redisClient *c, int nx) {
    int j, busykeys = 0;

    if ((c->argc % 2) == 0) {
        addReplySds(c,sdsnew("-ERR wrong number of arguments for MSET\r\n"));
        return;
    }
    /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set nothing at all if at least one already key exists. */
    if (nx) {
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                busykeys++;
            }
        }
    }
    if (busykeys) {
        addReply(c, shared.czero);
        return;
    }

    for (j = 1; j < c->argc; j += 2) {
        int retval;

        tryObjectEncoding(c->argv[j+1]);
        retval = dictAdd(c->db->dict,c->argv[j],c->argv[j+1]);
        if (retval == DICT_ERR) {
            dictReplace(c->db->dict,c->argv[j],c->argv[j+1]);
            incrRefCount(c->argv[j+1]);
        } else {
            incrRefCount(c->argv[j]);
            incrRefCount(c->argv[j+1]);
        }
        removeExpire(c->db,c->argv[j]);
    }
    server.dirty += (c->argc-1)/2;
    addReply(c, nx ? shared.cone : shared.ok);
}

static void msetCommand(redisClient *c) {
    msetGenericCommand(c,0);
}

static void msetnxCommand(redisClient *c) {
    msetGenericCommand(c,1);
}

static void incrDecrCommand(redisClient *c, long long incr) {
    long long value;
    int retval;
    robj *o;
    
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        value = 0;
    } else {
        if (o->type != REDIS_STRING) {
            value = 0;
        } else {
            char *eptr;

            if (o->encoding == REDIS_ENCODING_RAW)
                value = strtoll(o->ptr, &eptr, 10);
            else if (o->encoding == REDIS_ENCODING_INT)
                value = (long)o->ptr;
            else
                redisAssert(1 != 1);
        }
    }

    value += incr;
    o = createObject(REDIS_STRING,sdscatprintf(sdsempty(),"%lld",value));
    tryObjectEncoding(o);
    retval = dictAdd(c->db->dict,c->argv[1],o);
    if (retval == DICT_ERR) {
        dictReplace(c->db->dict,c->argv[1],o);
        removeExpire(c->db,c->argv[1]);
    } else {
        incrRefCount(c->argv[1]);
    }
    server.dirty++;
    addReply(c,shared.colon);
    addReply(c,o);
    addReply(c,shared.crlf);
}

static void incrCommand(redisClient *c) {
    incrDecrCommand(c,1);
}

static void decrCommand(redisClient *c) {
    incrDecrCommand(c,-1);
}

static void incrbyCommand(redisClient *c) {
    long long incr = strtoll(c->argv[2]->ptr, NULL, 10);
    incrDecrCommand(c,incr);
}

static void decrbyCommand(redisClient *c) {
    long long incr = strtoll(c->argv[2]->ptr, NULL, 10);
    incrDecrCommand(c,-incr);
}

static void appendCommand(redisClient *c) {
    int retval;
    size_t totlen;
    robj *o;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Create the key */
        retval = dictAdd(c->db->dict,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[1]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {
        dictEntry *de;
       
        de = dictFind(c->db->dict,c->argv[1]);
        assert(de != NULL);

        o = dictGetEntryVal(de);
        if (o->type != REDIS_STRING) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
        /* If the object is specially encoded or shared we have to make
         * a copy */
        if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW) {
            robj *decoded = getDecodedObject(o);

            o = createStringObject(decoded->ptr, sdslen(decoded->ptr));
            decrRefCount(decoded);
            dictReplace(c->db->dict,c->argv[1],o);
        }
        /* APPEND! */
        if (c->argv[2]->encoding == REDIS_ENCODING_RAW) {
            o->ptr = sdscatlen(o->ptr,
                c->argv[2]->ptr, sdslen(c->argv[2]->ptr));
        } else {
            o->ptr = sdscatprintf(o->ptr, "%ld",
                (unsigned long) c->argv[2]->ptr);
        }
        totlen = sdslen(o->ptr);
    }
    server.dirty++;
    addReplySds(c,sdscatprintf(sdsempty(),":%lu\r\n",(unsigned long)totlen));
}

static void substrCommand(redisClient *c) {
    robj *o;
    long start = atoi(c->argv[2]->ptr);
    long end = atoi(c->argv[3]->ptr);
    size_t rangelen, strlen;
    sds range;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_STRING)) return;

    o = getDecodedObject(o);
    strlen = sdslen(o->ptr);

    /* convert negative indexes */
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;

    /* indexes sanity checks */
    if (start > end || (size_t)start >= strlen) {
        /* Out of range start or start > end result in null reply */
        addReply(c,shared.nullbulk);
        decrRefCount(o);
        return;
    }
    if ((size_t)end >= strlen) end = strlen-1;
    rangelen = (end-start)+1;

    /* Return the result */
    addReplySds(c,sdscatprintf(sdsempty(),"$%zu\r\n",rangelen));
    range = sdsnewlen((char*)o->ptr+start,rangelen);
    addReplySds(c,range);
    addReply(c,shared.crlf);
    decrRefCount(o);
}

/* ========================= Type agnostic commands ========================= */

static void delCommand(redisClient *c) {
    int deleted = 0, j;

    for (j = 1; j < c->argc; j++) {
        if (deleteKey(c->db,c->argv[j])) {
            server.dirty++;
            deleted++;
        }
    }
    addReplyLong(c,deleted);
}

static void existsCommand(redisClient *c) {
    addReply(c,lookupKeyRead(c->db,c->argv[1]) ? shared.cone : shared.czero);
}

static void selectCommand(redisClient *c) {
    int id = atoi(c->argv[1]->ptr);
    
    if (selectDb(c,id) == REDIS_ERR) {
        addReplySds(c,sdsnew("-ERR invalid DB index\r\n"));
    } else {
        addReply(c,shared.ok);
    }
}

static void randomkeyCommand(redisClient *c) {
    dictEntry *de;
   
    while(1) {
        de = dictGetRandomKey(c->db->dict);
        if (!de || expireIfNeeded(c->db,dictGetEntryKey(de)) == 0) break;
    }
    if (de == NULL) {
        addReply(c,shared.plus);
        addReply(c,shared.crlf);
    } else {
        addReply(c,shared.plus);
        addReply(c,dictGetEntryKey(de));
        addReply(c,shared.crlf);
    }
}

static void keysCommand(redisClient *c) {
    dictIterator *di;
    dictEntry *de;
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern);
    unsigned long numkeys = 0;
    robj *lenobj = createObject(REDIS_STRING,NULL);

    di = dictGetIterator(c->db->dict);
    addReply(c,lenobj);
    decrRefCount(lenobj);
    while((de = dictNext(di)) != NULL) {
        robj *keyobj = dictGetEntryKey(de);

        sds key = keyobj->ptr;
        if ((pattern[0] == '*' && pattern[1] == '\0') ||
            stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            if (expireIfNeeded(c->db,keyobj) == 0) {
                addReplyBulk(c,keyobj);
                numkeys++;
            }
        }
    }
    dictReleaseIterator(di);
    lenobj->ptr = sdscatprintf(sdsempty(),"*%lu\r\n",numkeys);
}

static void dbsizeCommand(redisClient *c) {
    addReplySds(c,
        sdscatprintf(sdsempty(),":%lu\r\n",dictSize(c->db->dict)));
}

static void lastsaveCommand(redisClient *c) {
    addReplySds(c,
        sdscatprintf(sdsempty(),":%lu\r\n",server.lastsave));
}

static void typeCommand(redisClient *c) {
    robj *o;
    char *type;

    o = lookupKeyRead(c->db,c->argv[1]);
    if (o == NULL) {
        type = "+none";
    } else {
        switch(o->type) {
        case REDIS_STRING: type = "+string"; break;
        case REDIS_LIST: type = "+list"; break;
        case REDIS_SET: type = "+set"; break;
        case REDIS_ZSET: type = "+zset"; break;
        case REDIS_HASH: type = "+hash"; break;
        default: type = "+unknown"; break;
        }
    }
    addReplySds(c,sdsnew(type));
    addReply(c,shared.crlf);
}

static void saveCommand(redisClient *c) {
    if (server.bgsavechildpid != -1) {
        addReplySds(c,sdsnew("-ERR background save in progress\r\n"));
        return;
    }
    if (rdbSave(server.dbfilename) == REDIS_OK) {
        addReply(c,shared.ok);
    } else {
        addReply(c,shared.err);
    }
}

static void bgsaveCommand(redisClient *c) {
    if (server.bgsavechildpid != -1) {
        addReplySds(c,sdsnew("-ERR background save already in progress\r\n"));
        return;
    }
    if (rdbSaveBackground(server.dbfilename) == REDIS_OK) {
        char *status = "+Background saving started\r\n";
        addReplySds(c,sdsnew(status));
    } else {
        addReply(c,shared.err);
    }
}

static void shutdownCommand(redisClient *c) {
    redisLog(REDIS_WARNING,"User requested shutdown, saving DB...");
    /* Kill the saving child if there is a background saving in progress.
       We want to avoid race conditions, for instance our saving child may
       overwrite the synchronous saving did by SHUTDOWN. */
    if (server.bgsavechildpid != -1) {
        redisLog(REDIS_WARNING,"There is a live saving child. Killing it!");
        kill(server.bgsavechildpid,SIGKILL);
        rdbRemoveTempFile(server.bgsavechildpid);
    }
    if (server.appendonly) {
        /* Append only file: fsync() the AOF and exit */
        fsync(server.appendfd);
        if (server.vm_enabled) unlink(server.vm_swap_file);
        exit(0);
    } else {
        /* Snapshotting. Perform a SYNC SAVE and exit */
        if (rdbSave(server.dbfilename) == REDIS_OK) {
            if (server.daemonize)
                unlink(server.pidfile);
            redisLog(REDIS_WARNING,"%zu bytes used at exit",zmalloc_used_memory());
            redisLog(REDIS_WARNING,"Server exit now, bye bye...");
            if (server.vm_enabled) unlink(server.vm_swap_file);
            exit(0);
        } else {
            /* Ooops.. error saving! The best we can do is to continue
             * operating. Note that if there was a background saving process,
             * in the next cron() Redis will be notified that the background
             * saving aborted, handling special stuff like slaves pending for
             * synchronization... */
            redisLog(REDIS_WARNING,"Error trying to save the DB, can't exit"); 
            addReplySds(c,
                sdsnew("-ERR can't quit, problems saving the DB\r\n"));
        }
    }
}

static void renameGenericCommand(redisClient *c, int nx) {
    robj *o;

    /* To use the same key as src and dst is probably an error */
    if (sdscmp(c->argv[1]->ptr,c->argv[2]->ptr) == 0) {
        addReply(c,shared.sameobjecterr);
        return;
    }

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr)) == NULL)
        return;

    incrRefCount(o);
    deleteIfVolatile(c->db,c->argv[2]);
    if (dictAdd(c->db->dict,c->argv[2],o) == DICT_ERR) {
        if (nx) {
            decrRefCount(o);
            addReply(c,shared.czero);
            return;
        }
        dictReplace(c->db->dict,c->argv[2],o);
    } else {
        incrRefCount(c->argv[2]);
    }
    deleteKey(c->db,c->argv[1]);
    server.dirty++;
    addReply(c,nx ? shared.cone : shared.ok);
}

static void renameCommand(redisClient *c) {
    renameGenericCommand(c,0);
}

static void renamenxCommand(redisClient *c) {
    renameGenericCommand(c,1);
}

static void moveCommand(redisClient *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid;

    /* Obtain source and target DB pointers */
    src = c->db;
    srcid = c->db->id;
    if (selectDb(c,atoi(c->argv[2]->ptr)) == REDIS_ERR) {
        addReply(c,shared.outofrangeerr);
        return;
    }
    dst = c->db;
    selectDb(c,srcid); /* Back to the source DB */

    /* If the user is moving using as target the same
     * DB as the source DB it is probably an error. */
    if (src == dst) {
        addReply(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (!o) {
        addReply(c,shared.czero);
        return;
    }

    /* Try to add the element to the target DB */
    deleteIfVolatile(dst,c->argv[1]);
    if (dictAdd(dst->dict,c->argv[1],o) == DICT_ERR) {
        addReply(c,shared.czero);
        return;
    }
    incrRefCount(c->argv[1]);
    incrRefCount(o);

    /* OK! key moved, free the entry in the source DB */
    deleteKey(src,c->argv[1]);
    server.dirty++;
    addReply(c,shared.cone);
}

/* =================================== Lists ================================ */
static void pushGenericCommand(redisClient *c, int where) {
    robj *lobj;
    list *list;

    lobj = lookupKeyWrite(c->db,c->argv[1]);
    if (lobj == NULL) {
        if (handleClientsWaitingListPush(c,c->argv[1],c->argv[2])) {
            addReply(c,shared.cone);
            return;
        }
        lobj = createListObject();
        list = lobj->ptr;
        if (where == REDIS_HEAD) {
            listAddNodeHead(list,c->argv[2]);
        } else {
            listAddNodeTail(list,c->argv[2]);
        }
        dictAdd(c->db->dict,c->argv[1],lobj);
        incrRefCount(c->argv[1]);
        incrRefCount(c->argv[2]);
    } else {
        if (lobj->type != REDIS_LIST) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
        if (handleClientsWaitingListPush(c,c->argv[1],c->argv[2])) {
            addReply(c,shared.cone);
            return;
        }
        list = lobj->ptr;
        if (where == REDIS_HEAD) {
            listAddNodeHead(list,c->argv[2]);
        } else {
            listAddNodeTail(list,c->argv[2]);
        }
        incrRefCount(c->argv[2]);
    }
    server.dirty++;
    addReplySds(c,sdscatprintf(sdsempty(),":%d\r\n",listLength(list)));
}

static void lpushCommand(redisClient *c) {
    pushGenericCommand(c,REDIS_HEAD);
}

static void rpushCommand(redisClient *c) {
    pushGenericCommand(c,REDIS_TAIL);
}

static void llenCommand(redisClient *c) {
    robj *o;
    list *l;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_LIST)) return;
    
    l = o->ptr;
    addReplyUlong(c,listLength(l));
}

static void lindexCommand(redisClient *c) {
    robj *o;
    int index = atoi(c->argv[2]->ptr);
    list *list;
    listNode *ln;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_LIST)) return;
    list = o->ptr;

    ln = listIndex(list, index);
    if (ln == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        robj *ele = listNodeValue(ln);
        addReplyBulk(c,ele);
    }
}

static void lsetCommand(redisClient *c) {
    robj *o;
    int index = atoi(c->argv[2]->ptr);
    list *list;
    listNode *ln;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr)) == NULL ||
        checkType(c,o,REDIS_LIST)) return;
    list = o->ptr;

    ln = listIndex(list, index);
    if (ln == NULL) {
        addReply(c,shared.outofrangeerr);
    } else {
        robj *ele = listNodeValue(ln);

        decrRefCount(ele);
        listNodeValue(ln) = c->argv[3];
        incrRefCount(c->argv[3]);
        addReply(c,shared.ok);
        server.dirty++;
    }
}

static void popGenericCommand(redisClient *c, int where) {
    robj *o;
    list *list;
    listNode *ln;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_LIST)) return;
    list = o->ptr;

    if (where == REDIS_HEAD)
        ln = listFirst(list);
    else
        ln = listLast(list);

    if (ln == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        robj *ele = listNodeValue(ln);
        addReplyBulk(c,ele);
        listDelNode(list,ln);
        server.dirty++;
    }
}

static void lpopCommand(redisClient *c) {
    popGenericCommand(c,REDIS_HEAD);
}

static void rpopCommand(redisClient *c) {
    popGenericCommand(c,REDIS_TAIL);
}

static void lrangeCommand(redisClient *c) {
    robj *o;
    int start = atoi(c->argv[2]->ptr);
    int end = atoi(c->argv[3]->ptr);
    int llen;
    int rangelen, j;
    list *list;
    listNode *ln;
    robj *ele;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullmultibulk)) == NULL ||
        checkType(c,o,REDIS_LIST)) return;
    list = o->ptr;
    llen = listLength(list);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;

    /* indexes sanity checks */
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    ln = listIndex(list, start);
    addReplySds(c,sdscatprintf(sdsempty(),"*%d\r\n",rangelen));
    for (j = 0; j < rangelen; j++) {
        ele = listNodeValue(ln);
        addReplyBulk(c,ele);
        ln = ln->next;
    }
}

static void ltrimCommand(redisClient *c) {
    robj *o;
    int start = atoi(c->argv[2]->ptr);
    int end = atoi(c->argv[3]->ptr);
    int llen;
    int j, ltrim, rtrim;
    list *list;
    listNode *ln;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.ok)) == NULL ||
        checkType(c,o,REDIS_LIST)) return;
    list = o->ptr;
    llen = listLength(list);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;

    /* indexes sanity checks */
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    for (j = 0; j < ltrim; j++) {
        ln = listFirst(list);
        listDelNode(list,ln);
    }
    for (j = 0; j < rtrim; j++) {
        ln = listLast(list);
        listDelNode(list,ln);
    }
    server.dirty++;
    addReply(c,shared.ok);
}

static void lremCommand(redisClient *c) {
    robj *o;
    list *list;
    listNode *ln, *next;
    int toremove = atoi(c->argv[2]->ptr);
    int removed = 0;
    int fromtail = 0;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_LIST)) return;
    list = o->ptr;

    if (toremove < 0) {
        toremove = -toremove;
        fromtail = 1;
    }
    ln = fromtail ? list->tail : list->head;
    while (ln) {
        robj *ele = listNodeValue(ln);

        next = fromtail ? ln->prev : ln->next;
        if (compareStringObjects(ele,c->argv[3]) == 0) {
            listDelNode(list,ln);
            server.dirty++;
            removed++;
            if (toremove && removed == toremove) break;
        }
        ln = next;
    }
    addReplySds(c,sdscatprintf(sdsempty(),":%d\r\n",removed));
}

/* This is the semantic of this command:
 *  RPOPLPUSH srclist dstlist:
 *   IF LLEN(srclist) > 0
 *     element = RPOP srclist
 *     LPUSH dstlist element
 *     RETURN element
 *   ELSE
 *     RETURN nil
 *   END
 *  END
 *
 * The idea is to be able to get an element from a list in a reliable way
 * since the element is not just returned but pushed against another list
 * as well. This command was originally proposed by Ezra Zygmuntowicz.
 */
static void rpoplpushcommand(redisClient *c) {
    robj *sobj;
    list *srclist;
    listNode *ln;

    if ((sobj = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,sobj,REDIS_LIST)) return;
    srclist = sobj->ptr;
    ln = listLast(srclist);

    if (ln == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        robj *dobj = lookupKeyWrite(c->db,c->argv[2]);
        robj *ele = listNodeValue(ln);
        list *dstlist;

        if (dobj && dobj->type != REDIS_LIST) {
            addReply(c,shared.wrongtypeerr);
            return;
        }

        /* Add the element to the target list (unless it's directly
         * passed to some BLPOP-ing client */
        if (!handleClientsWaitingListPush(c,c->argv[2],ele)) {
            if (dobj == NULL) {
                /* Create the list if the key does not exist */
                dobj = createListObject();
                dictAdd(c->db->dict,c->argv[2],dobj);
                incrRefCount(c->argv[2]);
            }
            dstlist = dobj->ptr;
            listAddNodeHead(dstlist,ele);
            incrRefCount(ele);
        }

        /* Send the element to the client as reply as well */
        addReplyBulk(c,ele);

        /* Finally remove the element from the source list */
        listDelNode(srclist,ln);
        server.dirty++;
    }
}



/* ==================================== Sets ================================ */

static void saddCommand(redisClient *c) {
    robj *set;

    set = lookupKeyWrite(c->db,c->argv[1]);
    if (set == NULL) {
        set = createSetObject();
        dictAdd(c->db->dict,c->argv[1],set);
        incrRefCount(c->argv[1]);
    } else {
        if (set->type != REDIS_SET) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }
    if (dictAdd(set->ptr,c->argv[2],NULL) == DICT_OK) {
        incrRefCount(c->argv[2]);
        server.dirty++;
        addReply(c,shared.cone);
    } else {
        addReply(c,shared.czero);
    }
}

static void sremCommand(redisClient *c) {
    robj *set;

    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    if (dictDelete(set->ptr,c->argv[2]) == DICT_OK) {
        server.dirty++;
        if (htNeedsResize(set->ptr)) dictResize(set->ptr);
        addReply(c,shared.cone);
    } else {
        addReply(c,shared.czero);
    }
}

static void smoveCommand(redisClient *c) {
    robj *srcset, *dstset;

    srcset = lookupKeyWrite(c->db,c->argv[1]);
    dstset = lookupKeyWrite(c->db,c->argv[2]);

    /* If the source key does not exist return 0, if it's of the wrong type
     * raise an error */
    if (srcset == NULL || srcset->type != REDIS_SET) {
        addReply(c, srcset ? shared.wrongtypeerr : shared.czero);
        return;
    }
    /* Error if the destination key is not a set as well */
    if (dstset && dstset->type != REDIS_SET) {
        addReply(c,shared.wrongtypeerr);
        return;
    }
    /* Remove the element from the source set */
    if (dictDelete(srcset->ptr,c->argv[3]) == DICT_ERR) {
        /* Key not found in the src set! return zero */
        addReply(c,shared.czero);
        return;
    }
    server.dirty++;
    /* Add the element to the destination set */
    if (!dstset) {
        dstset = createSetObject();
        dictAdd(c->db->dict,c->argv[2],dstset);
        incrRefCount(c->argv[2]);
    }
    if (dictAdd(dstset->ptr,c->argv[3],NULL) == DICT_OK)
        incrRefCount(c->argv[3]);
    addReply(c,shared.cone);
}

static void sismemberCommand(redisClient *c) {
    robj *set;

    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    if (dictFind(set->ptr,c->argv[2]))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);
}

static void scardCommand(redisClient *c) {
    robj *o;
    dict *s;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_SET)) return;
    
    s = o->ptr;
    addReplyUlong(c,dictSize(s));
}

static void spopCommand(redisClient *c) {
    robj *set;
    dictEntry *de;

    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    de = dictGetRandomKey(set->ptr);
    if (de == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        robj *ele = dictGetEntryKey(de);

        addReplyBulk(c,ele);
        dictDelete(set->ptr,ele);
        if (htNeedsResize(set->ptr)) dictResize(set->ptr);
        server.dirty++;
    }
}

static void srandmemberCommand(redisClient *c) {
    robj *set;
    dictEntry *de;

    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    de = dictGetRandomKey(set->ptr);
    if (de == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        robj *ele = dictGetEntryKey(de);

        addReplyBulk(c,ele);
    }
}

static int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    dict **d1 = (void*) s1, **d2 = (void*) s2;

    return dictSize(*d1)-dictSize(*d2);
}

static void sinterGenericCommand(redisClient *c, robj **setskeys, unsigned long setsnum, robj *dstkey) {
    dict **dv = zmalloc(sizeof(dict*)*setsnum);
    dictIterator *di;
    dictEntry *de;
    robj *lenobj = NULL, *dstset = NULL;
    unsigned long j, cardinality = 0;

    for (j = 0; j < setsnum; j++) {
        robj *setobj;

        setobj = dstkey ?
                    lookupKeyWrite(c->db,setskeys[j]) :
                    lookupKeyRead(c->db,setskeys[j]);
        if (!setobj) {
            zfree(dv);
            if (dstkey) {
                if (deleteKey(c->db,dstkey))
                    server.dirty++;
                addReply(c,shared.czero);
            } else {
                addReply(c,shared.nullmultibulk);
            }
            return;
        }
        if (setobj->type != REDIS_SET) {
            zfree(dv);
            addReply(c,shared.wrongtypeerr);
            return;
        }
        dv[j] = setobj->ptr;
    }
    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performace */
    qsort(dv,setsnum,sizeof(dict*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    if (!dstkey) {
        lenobj = createObject(REDIS_STRING,NULL);
        addReply(c,lenobj);
        decrRefCount(lenobj);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        dstset = createSetObject();
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    di = dictGetIterator(dv[0]);

    while((de = dictNext(di)) != NULL) {
        robj *ele;

        for (j = 1; j < setsnum; j++)
            if (dictFind(dv[j],dictGetEntryKey(de)) == NULL) break;
        if (j != setsnum)
            continue; /* at least one set does not contain the member */
        ele = dictGetEntryKey(de);
        if (!dstkey) {
            addReplyBulk(c,ele);
            cardinality++;
        } else {
            dictAdd(dstset->ptr,ele,NULL);
            incrRefCount(ele);
        }
    }
    dictReleaseIterator(di);

    if (dstkey) {
        /* Store the resulting set into the target */
        deleteKey(c->db,dstkey);
        dictAdd(c->db->dict,dstkey,dstset);
        incrRefCount(dstkey);
    }

    if (!dstkey) {
        lenobj->ptr = sdscatprintf(sdsempty(),"*%lu\r\n",cardinality);
    } else {
        addReplySds(c,sdscatprintf(sdsempty(),":%lu\r\n",
            dictSize((dict*)dstset->ptr)));
        server.dirty++;
    }
    zfree(dv);
}

static void sinterCommand(redisClient *c) {
    sinterGenericCommand(c,c->argv+1,c->argc-1,NULL);
}

static void sinterstoreCommand(redisClient *c) {
    sinterGenericCommand(c,c->argv+2,c->argc-2,c->argv[1]);
}

#define REDIS_OP_UNION 0
#define REDIS_OP_DIFF 1
#define REDIS_OP_INTER 2

static void sunionDiffGenericCommand(redisClient *c, robj **setskeys, int setsnum, robj *dstkey, int op) {
    dict **dv = zmalloc(sizeof(dict*)*setsnum);
    dictIterator *di;
    dictEntry *de;
    robj *dstset = NULL;
    int j, cardinality = 0;

    for (j = 0; j < setsnum; j++) {
        robj *setobj;

        setobj = dstkey ?
                    lookupKeyWrite(c->db,setskeys[j]) :
                    lookupKeyRead(c->db,setskeys[j]);
        if (!setobj) {
            dv[j] = NULL;
            continue;
        }
        if (setobj->type != REDIS_SET) {
            zfree(dv);
            addReply(c,shared.wrongtypeerr);
            return;
        }
        dv[j] = setobj->ptr;
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    dstset = createSetObject();

    /* Iterate all the elements of all the sets, add every element a single
     * time to the result set */
    for (j = 0; j < setsnum; j++) {
        if (op == REDIS_OP_DIFF && j == 0 && !dv[j]) break; /* result set is empty */
        if (!dv[j]) continue; /* non existing keys are like empty sets */

        di = dictGetIterator(dv[j]);

        while((de = dictNext(di)) != NULL) {
            robj *ele;

            /* dictAdd will not add the same element multiple times */
            ele = dictGetEntryKey(de);
            if (op == REDIS_OP_UNION || j == 0) {
                if (dictAdd(dstset->ptr,ele,NULL) == DICT_OK) {
                    incrRefCount(ele);
                    cardinality++;
                }
            } else if (op == REDIS_OP_DIFF) {
                if (dictDelete(dstset->ptr,ele) == DICT_OK) {
                    cardinality--;
                }
            }
        }
        dictReleaseIterator(di);

        if (op == REDIS_OP_DIFF && cardinality == 0) break; /* result set is empty */
    }

    /* Output the content of the resulting set, if not in STORE mode */
    if (!dstkey) {
        addReplySds(c,sdscatprintf(sdsempty(),"*%d\r\n",cardinality));
        di = dictGetIterator(dstset->ptr);
        while((de = dictNext(di)) != NULL) {
            robj *ele;

            ele = dictGetEntryKey(de);
            addReplyBulk(c,ele);
        }
        dictReleaseIterator(di);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        deleteKey(c->db,dstkey);
        dictAdd(c->db->dict,dstkey,dstset);
        incrRefCount(dstkey);
    }

    /* Cleanup */
    if (!dstkey) {
        decrRefCount(dstset);
    } else {
        addReplySds(c,sdscatprintf(sdsempty(),":%lu\r\n",
            dictSize((dict*)dstset->ptr)));
        server.dirty++;
    }
    zfree(dv);
}

static void sunionCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,REDIS_OP_UNION);
}

static void sunionstoreCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],REDIS_OP_UNION);
}

static void sdiffCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,REDIS_OP_DIFF);
}

static void sdiffstoreCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],REDIS_OP_DIFF);
}
