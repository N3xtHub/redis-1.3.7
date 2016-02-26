/* ======================= Redis objects implementation ===================== */

static robj *createObject(int type, void *ptr) {
    robj *o;

    if (server.vm_enabled) pthread_mutex_lock(&server.obj_freelist_mutex);

    if (listLength(server.objfreelist)) {
        listNode *head = listFirst(server.objfreelist);
        o = listNodeValue(head);
        listDelNode(server.objfreelist,head);

        if (server.vm_enabled) pthread_mutex_unlock(&server.obj_freelist_mutex);
    } 

    o->type = type;
    o->encoding = REDIS_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = 1;
    if (server.vm_enabled) {
        /* Note that this code may run in the context of an I/O thread
         * and accessing to server.unixtime in theory is an error
         * (no locks). But in practice this is safe, and even if we read
         * garbage Redis will not fail, as it's just a statistical info */
        o->vm.atime = server.unixtime;
        o->storage = REDIS_VM_MEMORY;
    }
    return o;
}

static robj *createStringObject(char *ptr, size_t len) {
    return createObject(REDIS_STRING,sdsnewlen(ptr,len));
}

static robj *dupStringObject(robj *o) {
    assert(o->encoding == REDIS_ENCODING_RAW);
    return createStringObject(o->ptr,sdslen(o->ptr));
}

static robj *createListObject(void) {
    list *l = listCreate();

    listSetFreeMethod(l,decrRefCount);
    return createObject(REDIS_LIST,l);
}

static robj *createSetObject(void) {
    dict *d = dictCreate(&setDictType,NULL);
    return createObject(REDIS_SET,d);
}

static robj *createHashObject(void) {
    /* All the Hashes start as zipmaps. Will be automatically converted
     * into hash tables if there are enough elements or big elements
     * inside. */
    unsigned char *zm = zipmapNew();
    robj *o = createObject(REDIS_HASH,zm);
    o->encoding = REDIS_ENCODING_ZIPMAP;
    return o;
}

static robj *createZsetObject(void) {
    zset *zs = zmalloc(sizeof(*zs));

    zs->dict = dictCreate(&zsetDictType,NULL);
    zs->zsl = zslCreate();
    return createObject(REDIS_ZSET,zs);
}

static void freeStringObject(robj *o) {
    if (o->encoding == REDIS_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

static void freeListObject(robj *o) {
    listRelease((list*) o->ptr);
}

static void freeSetObject(robj *o) {
    dictRelease((dict*) o->ptr);
}

static void freeZsetObject(robj *o) {
    zset *zs = o->ptr;

    dictRelease(zs->dict);
    zslFree(zs->zsl);
    zfree(zs);
}

static void freeHashObject(robj *o) {
    switch (o->encoding) {
    case REDIS_ENCODING_HT:
        dictRelease((dict*) o->ptr);
        break;
    case REDIS_ENCODING_ZIPMAP:
        zfree(o->ptr);
        break;
    default:
        redisAssert(0);
        break;
    }
}

static void incrRefCount(robj *o) {
    redisAssert(!server.vm_enabled || o->storage == REDIS_VM_MEMORY);
    o->refcount++;
}

static void decrRefCount(void *obj) {
    robj *o = obj;

    /* Object is a key of a swapped out value, or in the process of being
     * loaded. */
    if (server.vm_enabled &&
        (o->storage == REDIS_VM_SWAPPED || o->storage == REDIS_VM_LOADING))
    {
        if (o->storage == REDIS_VM_SWAPPED || o->storage == REDIS_VM_LOADING) {
            redisAssert(o->refcount == 1);
        }
        if (o->storage == REDIS_VM_LOADING) vmCancelThreadedIOJob(obj);
        redisAssert(o->type == REDIS_STRING);
        freeStringObject(o);
        vmMarkPagesFree(o->vm.page,o->vm.usedpages);
        pthread_mutex_lock(&server.obj_freelist_mutex);
        if (listLength(server.objfreelist) > REDIS_OBJFREELIST_MAX ||
            !listAddNodeHead(server.objfreelist,o))
            zfree(o);
        pthread_mutex_unlock(&server.obj_freelist_mutex);
        server.vm_stats_swapped_objects--;
        return;
    }
    /* Object is in memory, or in the process of being swapped out. */
    if (--(o->refcount) == 0) {
        if (server.vm_enabled && o->storage == REDIS_VM_SWAPPING)
            vmCancelThreadedIOJob(obj);
        switch(o->type) {
        case REDIS_STRING: freeStringObject(o); break;
        case REDIS_LIST: freeListObject(o); break;
        case REDIS_SET: freeSetObject(o); break;
        case REDIS_ZSET: freeZsetObject(o); break;
        case REDIS_HASH: freeHashObject(o); break;
        default: redisAssert(0); break;
        }
        if (server.vm_enabled) pthread_mutex_lock(&server.obj_freelist_mutex);
        if (listLength(server.objfreelist) > REDIS_OBJFREELIST_MAX ||
            !listAddNodeHead(server.objfreelist,o))
            zfree(o);
        if (server.vm_enabled) pthread_mutex_unlock(&server.obj_freelist_mutex);
    }
}

static robj *lookupKey(redisDb *db, robj *key) {
    dictEntry *de = dictFind(db->dict,key);
    if (de) {
        robj *key = dictGetEntryKey(de);
        robj *val = dictGetEntryVal(de);

        if (server.vm_enabled) {
            if (key->storage == REDIS_VM_MEMORY ||
                key->storage == REDIS_VM_SWAPPING)
            {
                /* If we were swapping the object out, stop it, this key
                 * was requested. */
                if (key->storage == REDIS_VM_SWAPPING)
                    vmCancelThreadedIOJob(key);
                /* Update the access time of the key for the aging algorithm. */
                key->vm.atime = server.unixtime;
            } else {
                int notify = (key->storage == REDIS_VM_LOADING);

                /* Our value was swapped on disk. Bring it at home. */
                redisAssert(val == NULL);
                val = vmLoadObject(key);
                dictGetEntryVal(de) = val;

                /* Clients blocked by the VM subsystem may be waiting for
                 * this key... */
                if (notify) handleClientsBlockedOnSwappedKey(db,key);
            }
        }
        return val;
    } else {
        return NULL;
    }
}

static robj *lookupKeyRead(redisDb *db, robj *key) {
    expireIfNeeded(db,key);
    return lookupKey(db,key);
}

static robj *lookupKeyWrite(redisDb *db, robj *key) {
    deleteIfVolatile(db,key);
    return lookupKey(db,key);
}

static robj *lookupKeyReadOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = lookupKeyRead(c->db, key);
    if (!o) addReply(c,reply);
    return o;
}

static robj *lookupKeyWriteOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = lookupKeyWrite(c->db, key);
    if (!o) addReply(c,reply);
    return o;
}

static int checkType(redisClient *c, robj *o, int type) {
    if (o->type != type) {
        addReply(c,shared.wrongtypeerr);
        return 1;
    }
    return 0;
}

static int deleteKey(redisDb *db, robj *key) {
    int retval;

    /* We need to protect key from destruction: after the first dictDelete()
     * it may happen that 'key' is no longer valid if we don't increment
     * it's count. This may happen when we get the object reference directly
     * from the hash table with dictRandomKey() or dict iterators */
    incrRefCount(key);
    if (dictSize(db->expires)) dictDelete(db->expires,key);
    retval = dictDelete(db->dict,key);
    decrRefCount(key);

    return retval == DICT_OK;
}

/* Try to share an object against the shared objects pool */
static robj *tryObjectSharing(robj *o) {
    struct dictEntry *de;
    unsigned long c;

    if (o == NULL || server.shareobjects == 0) return o;

    de = dictFind(server.sharingpool,o);
    if (de) {
        robj *shared = dictGetEntryKey(de);

        c = ((unsigned long) dictGetEntryVal(de))+1;
        dictGetEntryVal(de) = (void*) c;
        incrRefCount(shared);
        decrRefCount(o);
        return shared;
    } else {
        /* Here we are using a stream algorihtm: Every time an object is
         * shared we increment its count, everytime there is a miss we
         * recrement the counter of a random object. If this object reaches
         * zero we remove the object and put the current object instead. */
        if (dictSize(server.sharingpool) >=
                server.sharingpoolsize) {
            de = dictGetRandomKey(server.sharingpool);
            
            c = ((unsigned long) dictGetEntryVal(de))-1;
            dictGetEntryVal(de) = (void*) c;
            if (c == 0) {
                dictDelete(server.sharingpool,de->key);
            }
        } else {
            c = 0; /* If the pool is empty we want to add this object */
        }
        if (c == 0) {
            int retval;

            retval = dictAdd(server.sharingpool,o,(void*)1);
            redisAssert(retval == DICT_OK);
            incrRefCount(o);
        }
        return o;
    }
}

/* Check if the nul-terminated string 's' can be represented by a long
 * (that is, is a number that fits into long without any other space or
 * character before or after the digits).
 *
 * If so, the function returns REDIS_OK and *longval is set to the value
 * of the number. Otherwise REDIS_ERR is returned */
static int isStringRepresentableAsLong(sds s, long *longval) {
    char buf[32], *endptr;
    long value;
    int slen;
    
    value = strtol(s, &endptr, 10);
    if (endptr[0] != '\0') return REDIS_ERR;
    slen = snprintf(buf,32,"%ld",value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    if (sdslen(s) != (unsigned)slen || memcmp(buf,s,slen)) return REDIS_ERR;
    if (longval) *longval = value;
    return REDIS_OK;
}

/* Try to encode a string object in order to save space */
static int tryObjectEncoding(robj *o) {
    long value;
    sds s = o->ptr;

    if (o->encoding != REDIS_ENCODING_RAW)
        return REDIS_ERR; /* Already encoded */

    /* It's not save to encode shared objects: shared objects can be shared
     * everywhere in the "object space" of Redis. Encoded objects can only
     * appear as "values" (and not, for instance, as keys) */
     if (o->refcount > 1) return REDIS_ERR;

    /* Currently we try to encode only strings */
    redisAssert(o->type == REDIS_STRING);

    /* Check if we can represent this string as a long integer */
    if (isStringRepresentableAsLong(s,&value) == REDIS_ERR) return REDIS_ERR;

    /* Ok, this object can be encoded */
    o->encoding = REDIS_ENCODING_INT;
    sdsfree(o->ptr);
    o->ptr = (void*) value;
    return REDIS_OK;
}

/* Get a decoded version of an encoded object (returned as a new object).
 * If the object is already raw-encoded just increment the ref count. */
static robj *getDecodedObject(robj *o) {
    robj *dec;
    
    if (o->encoding == REDIS_ENCODING_RAW) {
        incrRefCount(o);
        return o;
    }
    if (o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_INT) {
        char buf[32];

        snprintf(buf,32,"%ld",(long)o->ptr);
        dec = createStringObject(buf,strlen(buf));
        return dec;
    } else {
        redisAssert(1 != 1);
    }
}

/* Compare two string objects via strcmp() or alike.
 * Note that the objects may be integer-encoded. In such a case we
 * use snprintf() to get a string representation of the numbers on the stack
 * and compare the strings, it's much faster than calling getDecodedObject().
 *
 * Important note: if objects are not integer encoded, but binary-safe strings,
 * sdscmp() from sds.c will apply memcmp() so this function ca be considered
 * binary safe. */
static int compareStringObjects(robj *a, robj *b) {
    redisAssert(a->type == REDIS_STRING && b->type == REDIS_STRING);
    char bufa[128], bufb[128], *astr, *bstr;
    int bothsds = 1;

    if (a == b) return 0;
    if (a->encoding != REDIS_ENCODING_RAW) {
        snprintf(bufa,sizeof(bufa),"%ld",(long) a->ptr);
        astr = bufa;
        bothsds = 0;
    } else {
        astr = a->ptr;
    }
    if (b->encoding != REDIS_ENCODING_RAW) {
        snprintf(bufb,sizeof(bufb),"%ld",(long) b->ptr);
        bstr = bufb;
        bothsds = 0;
    } else {
        bstr = b->ptr;
    }
    return bothsds ? sdscmp(astr,bstr) : strcmp(astr,bstr);
}

static size_t stringObjectLen(robj *o) {
    redisAssert(o->type == REDIS_STRING);
    if (o->encoding == REDIS_ENCODING_RAW) {
        return sdslen(o->ptr);
    } else {
        char buf[32];

        return snprintf(buf,32,"%ld",(long)o->ptr);
    }
}
