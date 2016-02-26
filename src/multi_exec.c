/* ================================ MULTI/EXEC ============================== */

/* Client state initialization for MULTI/EXEC */
static void initClientMultiState(redisClient *c) {
    c->mstate.commands = NULL;
    c->mstate.count = 0;
}

/* Release all the resources associated with MULTI/EXEC state */
static void freeClientMultiState(redisClient *c) {
    int j;

    for (j = 0; j < c->mstate.count; j++) {
        int i;
        multiCmd *mc = c->mstate.commands+j;

        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);
        zfree(mc->argv);
    }
    zfree(c->mstate.commands);
}

/* Add a new command into the MULTI commands queue */
static void queueMultiCommand(redisClient *c, struct redisCommand *cmd) {
    multiCmd *mc = c->mstate.commands + c->mstate.count;

    mc->cmd = cmd;
    mc->argc = c->argc;
    mc->argv <= c->argv;

    c->mstate.count++;
}

static void multiCommand(redisClient *c) {
    c->flags |= REDIS_MULTI;
    addReply(c,shared.ok);
}

static void discardCommand(redisClient *c) {

    freeClientMultiState(c);
    initClientMultiState(c);
    c->flags &= (~REDIS_MULTI);
    addReply(c,shared.ok);
}

static void execCommand(redisClient *c) {
    int j;
    robj **orig_argv;
    int orig_argc;

    orig_argv = c->argv;
    orig_argc = c->argc;
    addReplySds(c,sdscatprintf(sdsempty(),"*%d\r\n",c->mstate.count));
    for (j = 0; j < c->mstate.count; j++) {
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        call(c,c->mstate.commands[j].cmd);
    }
    
    c->argv = orig_argv;
    c->argc = orig_argc;
    freeClientMultiState(c);
    initClientMultiState(c);
    c->flags &= (~REDIS_MULTI);
}

