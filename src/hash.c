// hash.c


/* =================================== Hashes =============================== */
static void hsetCommand(redisClient *c) {
    int update = 0;
    robj *o = lookupKeyWrite(c->db,c->argv[1]);

    if (o == NULL) {
        o = createHashObject();
        dictAdd(c->db->dict,c->argv[1],o);
        incrRefCount(c->argv[1]);
    } else {
        if (o->type != REDIS_HASH) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }
    /* We want to convert the zipmap into an hash table right now if the
     * entry to be added is too big. Note that we check if the object
     * is integer encoded before to try fetching the length in the test below.
     * This is because integers are small, but currently stringObjectLen()
     * performs a slow conversion: not worth it. */
    if (o->encoding == REDIS_ENCODING_ZIPMAP &&
        ((c->argv[2]->encoding == REDIS_ENCODING_RAW &&
          sdslen(c->argv[2]->ptr) > server.hash_max_zipmap_value) ||
         (c->argv[3]->encoding == REDIS_ENCODING_RAW &&
          sdslen(c->argv[3]->ptr) > server.hash_max_zipmap_value)))
    {
        convertToRealHash(o);
    }

    if (o->encoding == REDIS_ENCODING_ZIPMAP) {
        unsigned char *zm = o->ptr;
        robj *valobj = getDecodedObject(c->argv[3]);

        zm = zipmapSet(zm,c->argv[2]->ptr,sdslen(c->argv[2]->ptr),
            valobj->ptr,sdslen(valobj->ptr),&update);
        decrRefCount(valobj);
        o->ptr = zm;

        /* And here there is the second check for hash conversion...
         * we want to do it only if the operation was not just an update as
         * zipmapLen() is O(N). */
        if (!update && zipmapLen(zm) > server.hash_max_zipmap_entries)
            convertToRealHash(o);
    } else {
        tryObjectEncoding(c->argv[2]);
        /* note that c->argv[3] is already encoded, as the latest arg
         * of a bulk command is always integer encoded if possible. */
        if (dictReplace(o->ptr,c->argv[2],c->argv[3])) {
            incrRefCount(c->argv[2]);
        } else {
            update = 1;
        }
        incrRefCount(c->argv[3]);
    }
    server.dirty++;
    addReplySds(c,sdscatprintf(sdsempty(),":%d\r\n",update == 0));
}

static void hgetCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    if (o->encoding == REDIS_ENCODING_ZIPMAP) {
        unsigned char *zm = o->ptr;
        unsigned char *val;
        unsigned int vlen;
        robj *field;

        field = getDecodedObject(c->argv[2]);
        if (zipmapGet(zm,field->ptr,sdslen(field->ptr), &val,&vlen)) {
            addReplySds(c,sdscatprintf(sdsempty(),"$%u\r\n", vlen));
            addReplySds(c,sdsnewlen(val,vlen));
            addReply(c,shared.crlf);
            decrRefCount(field);
            return;
        } else {
            addReply(c,shared.nullbulk);
            decrRefCount(field);
            return;
        }
    } else {
        struct dictEntry *de;

        de = dictFind(o->ptr,c->argv[2]);
        if (de == NULL) {
            addReply(c,shared.nullbulk);
        } else {
            robj *e = dictGetEntryVal(de);

            addReplyBulk(c,e);
        }
    }
}

static void hdelCommand(redisClient *c) {
    robj *o;
    int deleted = 0;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    if (o->encoding == REDIS_ENCODING_ZIPMAP) {
        robj *field = getDecodedObject(c->argv[2]);

        o->ptr = zipmapDel((unsigned char*) o->ptr,
            (unsigned char*) field->ptr,
            sdslen(field->ptr), &deleted);
        decrRefCount(field);
    } else {
        deleted = dictDelete((dict*)o->ptr,c->argv[2]) == DICT_OK;
    }
    if (deleted) server.dirty++;
    addReply(c,deleted ? shared.cone : shared.czero);
}

static void hlenCommand(redisClient *c) {
    robj *o;
    unsigned long len;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    len = (o->encoding == REDIS_ENCODING_ZIPMAP) ?
            zipmapLen((unsigned char*)o->ptr) : dictSize((dict*)o->ptr);
    addReplyUlong(c,len);
}

#define REDIS_GETALL_KEYS 1
#define REDIS_GETALL_VALS 2
static void genericHgetallCommand(redisClient *c, int flags) {
    robj *o, *lenobj;
    unsigned long count = 0;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullmultibulk)) == NULL
        || checkType(c,o,REDIS_HASH)) return;

    lenobj = createObject(REDIS_STRING,NULL);
    addReply(c,lenobj);
    decrRefCount(lenobj);

    if (o->encoding == REDIS_ENCODING_ZIPMAP) {
        unsigned char *p = zipmapRewind(o->ptr);
        unsigned char *field, *val;
        unsigned int flen, vlen;

        while((p = zipmapNext(p,&field,&flen,&val,&vlen)) != NULL) {
            robj *aux;

            if (flags & REDIS_GETALL_KEYS) {
                aux = createStringObject((char*)field,flen);
                addReplyBulk(c,aux);
                decrRefCount(aux);
                count++;
            }
            if (flags & REDIS_GETALL_VALS) {
                aux = createStringObject((char*)val,vlen);
                addReplyBulk(c,aux);
                decrRefCount(aux);
                count++;
            }
        }
    } else {
        dictIterator *di = dictGetIterator(o->ptr);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            robj *fieldobj = dictGetEntryKey(de);
            robj *valobj = dictGetEntryVal(de);

            if (flags & REDIS_GETALL_KEYS) {
                addReplyBulk(c,fieldobj);
                count++;
            }
            if (flags & REDIS_GETALL_VALS) {
                addReplyBulk(c,valobj);
                count++;
            }
        }
        dictReleaseIterator(di);
    }
    lenobj->ptr = sdscatprintf(sdsempty(),"*%lu\r\n",count);
}

static void hkeysCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_GETALL_KEYS);
}

static void hvalsCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_GETALL_VALS);
}

static void hgetallCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_GETALL_KEYS|REDIS_GETALL_VALS);
}

static void hexistsCommand(redisClient *c) {
    robj *o;
    int exists = 0;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    if (o->encoding == REDIS_ENCODING_ZIPMAP) {
        robj *field;
        unsigned char *zm = o->ptr;

        field = getDecodedObject(c->argv[2]);
        exists = zipmapExists(zm,field->ptr,sdslen(field->ptr));
        decrRefCount(field);
    } else {
        exists = dictFind(o->ptr,c->argv[2]) != NULL;
    }
    addReply(c,exists ? shared.cone : shared.czero);
}

static void convertToRealHash(robj *o) {
    unsigned char *key, *val, *p, *zm = o->ptr;
    unsigned int klen, vlen;
    dict *dict = dictCreate(&hashDictType,NULL);

    assert(o->type == REDIS_HASH && o->encoding != REDIS_ENCODING_HT);
    p = zipmapRewind(zm);
    while((p = zipmapNext(p,&key,&klen,&val,&vlen)) != NULL) {
        robj *keyobj, *valobj;

        keyobj = createStringObject((char*)key,klen);
        valobj = createStringObject((char*)val,vlen);
        tryObjectEncoding(keyobj);
        tryObjectEncoding(valobj);
        dictAdd(dict,keyobj,valobj);
    }
    o->encoding = REDIS_ENCODING_HT;
    o->ptr = dict;
    zfree(zm);
}
