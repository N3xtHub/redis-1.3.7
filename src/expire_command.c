/* ================================= Expire ================================= */
static int removeExpire(redisDb *db, robj *key) {
    if (dictDelete(db->expires,key) == DICT_OK) {
        return 1;
    } else {
        return 0;
    }
}

static int setExpire(redisDb *db, robj *key, time_t when) {
    if (dictAdd(db->expires,key,(void*)when) == DICT_ERR) {
        return 0;
    } else {
        incrRefCount(key);
        return 1;
    }
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
static time_t getExpire(redisDb *db, robj *key) {
    dictEntry *de;

    /* No expire? return ASAP */
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key)) == NULL) return -1;

    return (time_t) dictGetEntryVal(de);
}

static int expireIfNeeded(redisDb *db, robj *key) {
    time_t when;
    dictEntry *de;

    /* No expire? return ASAP */
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key)) == NULL) return 0;

    /* Lookup the expire */
    when = (time_t) dictGetEntryVal(de);
    if (time(NULL) <= when) return 0;

    /* Delete the key */
    dictDelete(db->expires,key);
    return dictDelete(db->dict,key) == DICT_OK;
}

static int deleteIfVolatile(redisDb *db, robj *key) {
    dictEntry *de;

    /* No expire? return ASAP */
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key)) == NULL) return 0;

    /* Delete the key */
    server.dirty++;
    dictDelete(db->expires,key);
    return dictDelete(db->dict,key) == DICT_OK;
}

static void expireGenericCommand(redisClient *c, robj *key, time_t seconds) {
    dictEntry *de;

    de = dictFind(c->db->dict,key);
    if (de == NULL) {
        addReply(c,shared.czero);
        return;
    }
    if (seconds < 0) {
        if (deleteKey(c->db,key)) server.dirty++;
        addReply(c, shared.cone);
        return;
    } else {
        time_t when = time(NULL)+seconds;
        if (setExpire(c->db,key,when)) {
            addReply(c,shared.cone);
            server.dirty++;
        } else {
            addReply(c,shared.czero);
        }
        return;
    }
}

static void expireCommand(redisClient *c) {
    expireGenericCommand(c,c->argv[1],strtol(c->argv[2]->ptr,NULL,10));
}

static void expireatCommand(redisClient *c) {
    expireGenericCommand(c,c->argv[1],strtol(c->argv[2]->ptr,NULL,10)-time(NULL));
}

static void ttlCommand(redisClient *c) {
    time_t expire;
    int ttl = -1;

    expire = getExpire(c->db,c->argv[1]);
    if (expire != -1) {
        ttl = (int) (expire-time(NULL));
        if (ttl < 0) ttl = -1;
    }
    addReplySds(c,sdscatprintf(sdsempty(),":%d\r\n",ttl));
}