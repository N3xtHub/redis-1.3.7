// rdb.c

/*============================ RDB saving/loading =========================== */

static int rdbSaveType(FILE *fp, unsigned char type) {
    fwrite(&type,1,1,fp);
    return 0;
}

static int rdbSaveTime(FILE *fp, time_t t) {
    int32_t t32 = (int32_t) t;
    fwrite(&t32,4,1,fp);
    return 0;
}

/* check rdbLoadLen() comments for more info */
static int rdbSaveLen(FILE *fp, uint32_t len) {
    unsigned char buf[2];

    if (len < (1<<6)) {
        /* Save a 6 bit len */
        buf[0] = (len&0xFF)|(REDIS_RDB_6BITLEN<<6);
        fwrite(buf,1,1,fp);

    } else if (len < (1<<14)) {
        /* Save a 14 bit len */
        buf[0] = ((len>>8)&0xFF)|(REDIS_RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        fwrite(buf,2,1,fp) ;
    } else {
        /* Save a 32 bit len */
        buf[0] = (REDIS_RDB_32BITLEN<<6);
        fwrite(buf,1,1,fp);
        len = htonl(len);
        fwrite(&len,4,1,fp);
    }
    return 0;
}

/* String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space */
static int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc) {
    long long value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
    value = strtoll(s, &endptr, 10);
    if (endptr[0] != '\0') return 0;
    snprintf(buf,32,"%lld",value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    if (strlen(buf) != len || memcmp(buf,s,len)) return 0;

    /* Finally check if it fits in our ranges */
    if (value >= -(1<<7) && value <= (1<<7)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    } else {
        return 0;
    }
}

static int rdbSaveLzfStringObject(FILE *fp, unsigned char *s, size_t len) {
    size_t comprlen, outlen;
    unsigned char byte;
    void *out;

    /* We require at least four bytes compression for this to be worth it */
    if (len <= 4) return 0;
    outlen = len-4;
    
    comprlen = lzf_compress(s, len, out, outlen);
    
    /* Data compressed! Let's save it on disk */
    byte = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_LZF;

    fp << byte + comprlen + len + out;
    
    return comprlen;
}

/* Save a string objet as [len][data] on disk. If the object is a string
 * representation of an integer value we try to safe it in a special form */
static int rdbSaveRawString(FILE *fp, unsigned char *s, size_t len) {
    int enclen;

    /* Try integer encoding */
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) {
            fwrite(buf,enclen,1,fp) ;
            return 0;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it */
    if (server.rdbcompression && len > 20) {
        int retval;

        retval = rdbSaveLzfStringObject(fp,s,len);
        if (retval == -1) return -1;
        if (retval > 0) return 0;
        /* retval == 0 means data can't be compressed, save the old way */
    }

    /* Store verbatim */
    if (rdbSaveLen(fp,len) == -1) return -1;
    if (len && fwrite(s,len,1,fp) == 0) return -1;
    return 0;
}

/* Like rdbSaveStringObjectRaw() but handle encoded objects */
static int rdbSaveStringObject(FILE *fp, robj *obj) {
    int retval;

    /* Avoid incr/decr ref count business when possible.
     * This plays well with copy-on-write given that we are probably
     * in a child process (BGSAVE). Also this makes sure key objects
     * of swapped objects are not incRefCount-ed (an assert does not allow
     * this in order to avoid bugs) */
    if (obj->encoding != REDIS_ENCODING_RAW) {
        obj = getDecodedObject(obj);
        retval = rdbSaveRawString(fp,obj->ptr,sdslen(obj->ptr));
        decrRefCount(obj);
    } else {
        retval = rdbSaveRawString(fp,obj->ptr,sdslen(obj->ptr));
    }
    return retval;
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifing the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
static int rdbSaveDoubleValue(FILE *fp, double val) {
    unsigned char buf[128];
    int len;

    if (isnan(val)) {
        buf[0] = 253;
        len = 1;
    } else if (!isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
        snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    if (fwrite(buf,len,1,fp) == 0) return -1;
    return 0;
}

/* Save a Redis object. */
static int rdbSaveObject(FILE *fp, robj *o) {
    if (o->type == REDIS_STRING) {
        /* Save a string value */
        if (rdbSaveStringObject(fp,o) == -1) return -1;
    } else if (o->type == REDIS_LIST) {
        /* Save a list value */
        list *list = o->ptr;
        listIter li;
        listNode *ln;

        if (rdbSaveLen(fp,listLength(list)) == -1) return -1;
        listRewind(list,&li);
        while((ln = listNext(&li))) {
            robj *eleobj = listNodeValue(ln);

            if (rdbSaveStringObject(fp,eleobj) == -1) return -1;
        }
    } else if (o->type == REDIS_SET) {
        /* Save a set value */
        dict *set = o->ptr;
        dictIterator *di = dictGetIterator(set);
        dictEntry *de;

        if (rdbSaveLen(fp,dictSize(set)) == -1) return -1;
        while((de = dictNext(di)) != NULL) {
            robj *eleobj = dictGetEntryKey(de);

            if (rdbSaveStringObject(fp,eleobj) == -1) return -1;
        }
        dictReleaseIterator(di);
    } else if (o->type == REDIS_ZSET) {
        /* Save a set value */
        zset *zs = o->ptr;
        dictIterator *di = dictGetIterator(zs->dict);
        dictEntry *de;

        if (rdbSaveLen(fp,dictSize(zs->dict)) == -1) return -1;
        while((de = dictNext(di)) != NULL) {
            robj *eleobj = dictGetEntryKey(de);
            double *score = dictGetEntryVal(de);

            if (rdbSaveStringObject(fp,eleobj) == -1) return -1;
            if (rdbSaveDoubleValue(fp,*score) == -1) return -1;
        }
        dictReleaseIterator(di);
    } else if (o->type == REDIS_HASH) {
        /* Save a hash value */
        if (o->encoding == REDIS_ENCODING_ZIPMAP) {
            unsigned char *p = zipmapRewind(o->ptr);
            unsigned int count = zipmapLen(o->ptr);
            unsigned char *key, *val;
            unsigned int klen, vlen;

            if (rdbSaveLen(fp,count) == -1) return -1;
            while((p = zipmapNext(p,&key,&klen,&val,&vlen)) != NULL) {
                if (rdbSaveRawString(fp,key,klen) == -1) return -1;
                if (rdbSaveRawString(fp,val,vlen) == -1) return -1;
            }
        } else {
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            if (rdbSaveLen(fp,dictSize((dict*)o->ptr)) == -1) return -1;
            while((de = dictNext(di)) != NULL) {
                robj *key = dictGetEntryKey(de);
                robj *val = dictGetEntryVal(de);

                if (rdbSaveStringObject(fp,key) == -1) return -1;
                if (rdbSaveStringObject(fp,val) == -1) return -1;
            }
            dictReleaseIterator(di);
        }
    } else {
        redisAssert(0);
    }
    return 0;
}

/* Return the length the object will have on disk if saved with
 * the rdbSaveObject() function. Currently we use a trick to get
 * this length with very little changes to the code. In the future
 * we could switch to a faster solution. */
static off_t rdbSavedObjectLen(robj *o, FILE *fp) {
    if (fp == NULL) fp = server.devnull;
    rewind(fp);
    assert(rdbSaveObject(fp,o) != 1);
    return ftello(fp);
}

/* Return the number of pages required to save this object in the swap file */
static off_t rdbSavedObjectPages(robj *o, FILE *fp) {
    off_t bytes = rdbSavedObjectLen(o,fp);
    
    return (bytes+(server.vm_page_size-1))/server.vm_page_size;
}

/* Save the DB on disk. Return REDIS_ERR on error, REDIS_OK on success */
static int rdbSave(char *filename) {
    dictIterator *di = NULL;
    dictEntry *de;
    FILE *fp;
    char tmpfile[256];
    int j;
    time_t now = time(NULL);

    /* Wait for I/O therads to terminate, just in case this is a
     * foreground-saving, to avoid seeking the swap file descriptor at the
     * same time. */
    if (server.vm_enabled)
        waitEmptyIOJobsQueue();

    snprintf(tmpfile,256,"temp-%d.rdb", (int) getpid());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        redisLog(REDIS_WARNING, "Failed saving the DB: %s", strerror(errno));
        return REDIS_ERR;
    }
    if (fwrite("REDIS0001",9,1,fp) == 0) goto werr;
    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;
        di = dictGetIterator(d);

        /* Write the SELECT DB opcode */
        if (rdbSaveType(fp,REDIS_SELECTDB) == -1) goto werr;
        if (rdbSaveLen(fp,j) == -1) goto werr;

        /* Iterate this DB writing every entry */
        while((de = dictNext(di)) != NULL) {
            robj *key = dictGetEntryKey(de);
            robj *o = dictGetEntryVal(de);
            time_t expiretime = getExpire(db,key);

            /* Save the expire time */
            if (expiretime != -1) {
                /* If this key is already expired skip it */
                if (expiretime < now) continue;
                if (rdbSaveType(fp,REDIS_EXPIRETIME) == -1) goto werr;
                if (rdbSaveTime(fp,expiretime) == -1) goto werr;
            }
            /* Save the key and associated value. This requires special
             * handling if the value is swapped out. */
            if (!server.vm_enabled || key->storage == REDIS_VM_MEMORY ||
                                      key->storage == REDIS_VM_SWAPPING) {
                /* Save type, key, value */
                if (rdbSaveType(fp,o->type) == -1) goto werr;
                if (rdbSaveStringObject(fp,key) == -1) goto werr;
                if (rdbSaveObject(fp,o) == -1) goto werr;
            } else {
                /* REDIS_VM_SWAPPED or REDIS_VM_LOADING */
                robj *po;
                /* Get a preview of the object in memory */
                po = vmPreviewObject(key);
                /* Save type, key, value */
                if (rdbSaveType(fp,key->vtype) == -1) goto werr;
                if (rdbSaveStringObject(fp,key) == -1) goto werr;
                if (rdbSaveObject(fp,po) == -1) goto werr;
                /* Remove the loaded object from memory */
                decrRefCount(po);
            }
        }
        dictReleaseIterator(di);
    }
    /* EOF opcode */
    if (rdbSaveType(fp,REDIS_EOF) == -1) goto werr;

    /* Make sure data will not remain on the OS's output buffers */
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);
    
    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    if (rename(tmpfile,filename) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp DB file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return REDIS_ERR;
    }
    redisLog(REDIS_NOTICE,"DB saved on disk");
    server.dirty = 0;
    server.lastsave = time(NULL);
    return REDIS_OK;

werr:
    fclose(fp);
    unlink(tmpfile);
    redisLog(REDIS_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    if (di) dictReleaseIterator(di);
    return REDIS_ERR;
}

static int rdbSaveBackground(char *filename) {
    pid_t childpid;

    if (server.bgsavechildpid != -1) return REDIS_ERR;
    if (server.vm_enabled) waitEmptyIOJobsQueue();
    if ((childpid = fork()) == 0) {
        /* Child */
        if (server.vm_enabled) vmReopenSwapFile();
        close(server.fd);
        if (rdbSave(filename) == REDIS_OK) {
            _exit(0);
        } else {
            _exit(1);
        }
    } else {
        /* Parent */
        if (childpid == -1) {
            redisLog(REDIS_WARNING,"Can't save in background: fork: %s",
                strerror(errno));
            return REDIS_ERR;
        }
        redisLog(REDIS_NOTICE,"Background saving started by pid %d",childpid);
        server.bgsavechildpid = childpid;
        return REDIS_OK;
    }
    return REDIS_OK; /* unreached */
}

static void rdbRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,256,"temp-%d.rdb", (int) childpid);
    unlink(tmpfile);
}

static int rdbLoadType(FILE *fp) {
    unsigned char type;
    if (fread(&type,1,1,fp) == 0) return -1;
    return type;
}

static time_t rdbLoadTime(FILE *fp) {
    int32_t t32;
    if (fread(&t32,4,1,fp) == 0) return -1;
    return (time_t) t32;
}

/* Load an encoded length from the DB, see the REDIS_RDB_* defines on the top
 * of this file for a description of how this are stored on disk.
 *
 * isencoded is set to 1 if the readed length is not actually a length but
 * an "encoding type", check the above comments for more info */
static uint32_t rdbLoadLen(FILE *fp, int *isencoded) {
    unsigned char buf[2];
    uint32_t len;
    int type;

    if (isencoded) *isencoded = 0;
    if (fread(buf,1,1,fp) == 0) return REDIS_RDB_LENERR;
    type = (buf[0]&0xC0)>>6;
    if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len */
        return buf[0]&0x3F;
    } else if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit len encoding type */
        if (isencoded) *isencoded = 1;
        return buf[0]&0x3F;
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len */
        if (fread(buf+1,1,1,fp) == 0) return REDIS_RDB_LENERR;
        return ((buf[0]&0x3F)<<8)|buf[1];
    } else {
        /* Read a 32 bit len */
        if (fread(&len,4,1,fp) == 0) return REDIS_RDB_LENERR;
        return ntohl(len);
    }
}

static robj *rdbLoadIntegerObject(FILE *fp, int enctype) {
    unsigned char enc[4];
    long long val;

    if (enctype == REDIS_RDB_ENC_INT8) {
        if (fread(enc,1,1,fp) == 0) return NULL;
        val = (signed char)enc[0];
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (fread(enc,2,1,fp) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (fread(enc,4,1,fp) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        redisAssert(0);
    }
    return createObject(REDIS_STRING,sdscatprintf(sdsempty(),"%lld",val));
}

static robj *rdbLoadLzfStringObject(FILE*fp) {
    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;

    if ((clen = rdbLoadLen(fp,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(fp,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((c = zmalloc(clen)) == NULL) goto err;
    if ((val = sdsnewlen(NULL,len)) == NULL) goto err;
    if (fread(c,clen,1,fp) == 0) goto err;
    if (lzf_decompress(c,clen,val,len) == 0) goto err;
    zfree(c);
    return createObject(REDIS_STRING,val);
err:
    zfree(c);
    sdsfree(val);
    return NULL;
}

static robj *rdbLoadStringObject(FILE*fp) {
    int isencoded;
    uint32_t len;
    sds val;

    len = rdbLoadLen(fp,&isencoded);
    if (isencoded) {
        switch(len) {
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return tryObjectSharing(rdbLoadIntegerObject(fp,len));
        case REDIS_RDB_ENC_LZF:
            return tryObjectSharing(rdbLoadLzfStringObject(fp));
        default:
            redisAssert(0);
        }
    }

    if (len == REDIS_RDB_LENERR) return NULL;
    val = sdsnewlen(NULL,len);
    if (len && fread(val,len,1,fp) == 0) {
        sdsfree(val);
        return NULL;
    }
    return tryObjectSharing(createObject(REDIS_STRING,val));
}

/* For information about double serialization check rdbSaveDoubleValue() */
static int rdbLoadDoubleValue(FILE *fp, double *val) {
    char buf[128];
    unsigned char len;

    if (fread(&len,1,1,fp) == 0) return -1;
    switch(len) {
    case 255: *val = R_NegInf; return 0;
    case 254: *val = R_PosInf; return 0;
    case 253: *val = R_Nan; return 0;
    default:
        if (fread(buf,len,1,fp) == 0) return -1;
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        return 0;
    }
}

/* Load a Redis object of the specified type from the specified file.
 * On success a newly allocated object is returned, otherwise NULL. */
static robj *rdbLoadObject(int type, FILE *fp) {
    robj *o;

    redisLog(REDIS_DEBUG,"LOADING OBJECT %d (at %d)\n",type,ftell(fp));
    if (type == REDIS_STRING) {
        /* Read string value */
        if ((o = rdbLoadStringObject(fp)) == NULL) return NULL;
        tryObjectEncoding(o);
    } else if (type == REDIS_LIST || type == REDIS_SET) {
        /* Read list/set value */
        uint32_t listlen;

        if ((listlen = rdbLoadLen(fp,NULL)) == REDIS_RDB_LENERR) return NULL;
        o = (type == REDIS_LIST) ? createListObject() : createSetObject();
        /* It's faster to expand the dict to the right size asap in order
         * to avoid rehashing */
        if (type == REDIS_SET && listlen > DICT_HT_INITIAL_SIZE)
            dictExpand(o->ptr,listlen);
        /* Load every single element of the list/set */
        while(listlen--) {
            robj *ele;

            if ((ele = rdbLoadStringObject(fp)) == NULL) return NULL;
            tryObjectEncoding(ele);
            if (type == REDIS_LIST) {
                listAddNodeTail((list*)o->ptr,ele);
            } else {
                dictAdd((dict*)o->ptr,ele,NULL);
            }
        }
    } else if (type == REDIS_ZSET) {
        /* Read list/set value */
        size_t zsetlen;
        zset *zs;

        if ((zsetlen = rdbLoadLen(fp,NULL)) == REDIS_RDB_LENERR) return NULL;
        o = createZsetObject();
        zs = o->ptr;
        /* Load every single element of the list/set */
        while(zsetlen--) {
            robj *ele;
            double *score = zmalloc(sizeof(double));

            if ((ele = rdbLoadStringObject(fp)) == NULL) return NULL;
            tryObjectEncoding(ele);
            if (rdbLoadDoubleValue(fp,score) == -1) return NULL;
            dictAdd(zs->dict,ele,score);
            zslInsert(zs->zsl,*score,ele);
            incrRefCount(ele); /* added to skiplist */
        }
    } else if (type == REDIS_HASH) {
        size_t hashlen;

        if ((hashlen = rdbLoadLen(fp,NULL)) == REDIS_RDB_LENERR) return NULL;
        o = createHashObject();
        /* Too many entries? Use an hash table. */
        if (hashlen > server.hash_max_zipmap_entries)
            convertToRealHash(o);
        /* Load every key/value, then set it into the zipmap or hash
         * table, as needed. */
        while(hashlen--) {
            robj *key, *val;

            if ((key = rdbLoadStringObject(fp)) == NULL) return NULL;
            if ((val = rdbLoadStringObject(fp)) == NULL) return NULL;
            /* If we are using a zipmap and there are too big values
             * the object is converted to real hash table encoding. */
            if (o->encoding != REDIS_ENCODING_HT &&
               (sdslen(key->ptr) > server.hash_max_zipmap_value ||
                sdslen(val->ptr) > server.hash_max_zipmap_value))
            {
                    convertToRealHash(o);
            }

            if (o->encoding == REDIS_ENCODING_ZIPMAP) {
                unsigned char *zm = o->ptr;

                zm = zipmapSet(zm,key->ptr,sdslen(key->ptr),
                                  val->ptr,sdslen(val->ptr),NULL);
                o->ptr = zm;
                decrRefCount(key);
                decrRefCount(val);
            } else {
                tryObjectEncoding(key);
                tryObjectEncoding(val);
                dictAdd((dict*)o->ptr,key,val);
            }
        }
    } else {
        redisAssert(0);
    }
    return o;
}

static int rdbLoad(char *filename) {
    FILE *fp;
    robj *keyobj = NULL;
    uint32_t dbid;
    int type, retval, rdbver;
    dict *d = server.db[0].dict;
    redisDb *db = server.db+0;
    char buf[1024];
    time_t expiretime = -1, now = time(NULL);
    long long loadedkeys = 0;

    fp = fopen(filename,"r");
    fread(buf,9,1,fp) ;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        return REDIS_ERR;
    }

    rdbver = atoi(buf+5);
    while(1) {
        robj *o;

        /* Read type. */
        if ((type = rdbLoadType(fp)) == -1) goto eoferr;
        if (type == REDIS_EXPIRETIME) {
            if ((expiretime = rdbLoadTime(fp)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again */
            if ((type = rdbLoadType(fp)) == -1) goto eoferr;
        }
        if (type == REDIS_EOF) break;
        /* Handle SELECT DB opcode as a special case */
        if (type == REDIS_SELECTDB) {
            dbid = rdbLoadLen(fp,NULL);
            db = server.db + dbid;
            d = db->dict;
            continue;
        }
        /* Read key */
        keyobj = rdbLoadStringObject(fp);
        /* Read value */
        o = rdbLoadObject(type,fp);
        /* Add the new object in the hash table */
        retval = dictAdd(d,keyobj,o);
 
        /* Set the expire time if needed */
        if (expiretime != -1) {
            setExpire(db,keyobj,expiretime);
            /* Delete this key if already expired */
            if (expiretime < now) deleteKey(db,keyobj);
            expiretime = -1;
        }
        keyobj = o = NULL;
        /* Handle swapping while loading big datasets when VM is on */
        loadedkeys++;
        if (server.vm_enabled && (loadedkeys % 5000) == 0) {
            while (zmalloc_used_memory() > server.vm_max_memory) {
                if (vmSwapOneObjectBlocking() == REDIS_ERR) break;
            }
        }
    }
    fclose(fp);
    return REDIS_OK;
}