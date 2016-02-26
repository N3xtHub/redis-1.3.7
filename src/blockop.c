/* =========================== Blocking Operations  ========================= */

/* Currently Redis blocking operations support is limited to list POP ops,
 * so the current implementation is not fully generic, but it is also not
 * completely specific so it will not require a rewrite to support new
 * kind of blocking operations in the future.
 *
 * Still it's important to note that list blocking operations can be already
 * used as a notification mechanism in order to implement other blocking
 * operations at application level, so there must be a very strong evidence
 * of usefulness and generality before new blocking operations are implemented.
 *
 * This is how the current blocking POP works, we use BLPOP as example:
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP
 *   if there is not to block.
 * - If instead BLPOP is called and the key does not exists or the list is
 *   empty we need to block. In order to do so we remove the notification for
 *   new data to read in the client socket (so that we'll not serve new
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary (db->blockingkeys) mapping keys to a list of clients
 *   blocking for this keys.
 * - If a PUSH operation against a key with blocked clients waiting is
 *   performed, we serve the first in the list: basically instead to push
 *   the new element inside the list we return it to the (first / oldest)
 *   blocking client, unblock the client, and remove it form the list.
 *
 * The above comment and the source code should be enough in order to understand
 * the implementation and modify / fix it later.
 */

/* Set a client in blocking mode for the specified key, with the specified
 * timeout */
static void blockForKeys(redisClient *c, robj **keys, int numkeys, time_t timeout) {
    dictEntry *de;
    list *l;
    int j;

    c->blockingkeys = zmalloc(sizeof(robj*)*numkeys);
    c->blockingkeysnum = numkeys;
    c->blockingto = timeout;
    for (j = 0; j < numkeys; j++) {
        /* Add the key in the client structure, to map clients -> keys */
        c->blockingkeys[j] = keys[j];
        incrRefCount(keys[j]);

        /* And in the other "side", to map keys -> clients */
        de = dictFind(c->db->blockingkeys, keys[j]);
        if (de == NULL) {
            int retval;

            /* For every key we take a list of clients blocked for it */
            l = listCreate();
            retval = dictAdd(c->db->blockingkeys,keys[j],l);
            incrRefCount(keys[j]);
        } else {
            l = dictGetEntryVal(de);
        }

        listAddNodeTail(l, c);
    }
    /* Mark the client as a blocked client */
    c->flags |= REDIS_BLOCKED;
    server.blpop_blocked_clients++;
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP */
static void unblockClientWaitingData(redisClient *c) {
    dictEntry *de;
    list *l;
    int j;

    assert(c->blockingkeys != NULL);
    /* The client may wait for multiple keys, so unblock it for every key. */
    for (j = 0; j < c->blockingkeysnum; j++) {
        /* Remove this client from the list of clients waiting for this key. */
        de = dictFind(c->db->blockingkeys,c->blockingkeys[j]);
        
        l = dictGetEntryVal(de);
        listDelNode(l,listSearchKey(l,c));
        /* If the list is empty we need to remove it to avoid wasting memory */
        if (listLength(l) == 0)
            dictDelete(c->db->blockingkeys,c->blockingkeys[j]);
        decrRefCount(c->blockingkeys[j]);
    }
    /* Cleanup the client structure */
    zfree(c->blockingkeys);
    c->blockingkeys = NULL;
    c->flags &= (~REDIS_BLOCKED);
    server.blpop_blocked_clients--;
    /* We want to process data if there is some command waiting
     * in the input buffer. Note that this is safe even if
     * unblockClientWaitingData() gets called from freeClient() because
     * freeClient() will be smart enough to call this function
     * *after* c->querybuf was set to NULL. */
    if (c->querybuf && sdslen(c->querybuf) > 0) processInputBuffer(c);
}

/* This should be called from any function PUSHing into lists.
 * 'c' is the "pushing client", 'key' is the key it is pushing data against,
 * 'ele' is the element pushed.
 *
 * If the function returns 0 there was no client waiting for a list push
 * against this key.
 *
 * If the function returns 1 there was a client waiting for a list push
 * against this key, the element was passed to this client thus it's not
 * needed to actually add it to the list and the caller should return asap. */
static int handleClientsWaitingListPush(redisClient *c, robj *key, robj *ele) {
    struct dictEntry *de;
    redisClient *receiver;
    list *l;
    listNode *ln;

    de = dictFind(c->db->blockingkeys,key);
    if (de == NULL) return 0;
    l = dictGetEntryVal(de);
    ln = listFirst(l);
    
    receiver = ln->value;

    addReplySds(receiver,sdsnew("*2\r\n"));
    addReplyBulk(receiver,key);
    addReplyBulk(receiver,ele);
    unblockClientWaitingData(receiver);
    return 1;
}

/* Blocking RPOP/LPOP */
static void blockingPopGenericCommand(redisClient *c, int where) {
    robj *o;
    time_t timeout;
    int j;

    for (j = 1; j < c->argc-1; j++) {
        o = lookupKeyWrite(c->db,c->argv[j]);
        if (o != NULL) {
            if (o->type != REDIS_LIST) {
                addReply(c,shared.wrongtypeerr);
                return;
            } else {
                list *list = o->ptr;
                if (listLength(list) != 0) {
                    /* If the list contains elements fall back to the usual
                     * non-blocking POP operation */
                    robj *argv[2], **orig_argv;
                    int orig_argc;
                   
                    /* We need to alter the command arguments before to call
                     * popGenericCommand() as the command takes a single key. */
                    orig_argv = c->argv;
                    orig_argc = c->argc;
                    argv[1] = c->argv[j];
                    c->argv = argv;
                    c->argc = 2;

                    /* Also the return value is different, we need to output
                     * the multi bulk reply header and the key name. The
                     * "real" command will add the last element (the value)
                     * for us. If this souds like an hack to you it's just
                     * because it is... */
                    addReplySds(c,sdsnew("*2\r\n"));
                    addReplyBulk(c,argv[1]);
                    popGenericCommand(c,where);

                    /* Fix the client structure with the original stuff */
                    c->argv = orig_argv;
                    c->argc = orig_argc;
                    return;
                }
            }
        }
    }
    /* If the list is empty or the key does not exists we must block */
    timeout = strtol(c->argv[c->argc-1]->ptr,NULL,10);
    if (timeout > 0) timeout += time(NULL);
    blockForKeys(c,c->argv+1,c->argc-2,timeout);
}

static void blpopCommand(redisClient *c) {
    blockingPopGenericCommand(c,REDIS_HEAD);
}

static void brpopCommand(redisClient *c) {
    blockingPopGenericCommand(c,REDIS_TAIL);
}

