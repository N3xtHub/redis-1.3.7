

/* Virtual Memory is composed mainly of two subsystems:
 * - Blocking Virutal Memory
 * - Threaded Virtual Memory I/O
 * The two parts are not fully decoupled, but functions are split among two
 * different sections of the source code (delimited by comments) in order to
 * make more clear what functionality is about the blocking VM and what about
 * the threaded (not blocking) VM.
 *
 * Redis VM design:
 *
 * Redis VM is a blocking VM (one that blocks reading swapped values from
 * disk into memory when a value swapped out is needed in memory) that is made
 * unblocking by trying to examine the command argument vector in order to
 * load in background values that will likely be needed in order to exec
 * the command. The command is executed only once all the relevant keys
 * are loaded into memory.
 *
 * This basically is almost as simple of a blocking VM, but almost as parallel
 * as a fully non-blocking VM.
 */

/* =================== Virtual Memory - Blocking Side  ====================== */

/* substitute the first occurrence of '%p' with the process pid in the
 * swap file name. */
static void expandVmSwapFilename(void) {
    char *p = strstr(server.vm_swap_file,"%p");
    sds new;
    
    new = sdsempty();
    *p = '\0';
    new = sdscat(new,server.vm_swap_file);
    new = sdscatprintf(new,"%ld",(long) getpid());
    new = sdscat(new,p+2);
    zfree(server.vm_swap_file);
    server.vm_swap_file = new;
}

static void vmInit(void) {
    off_t totsize;
    int pipefds[2];
    size_t stacksize;

    if (server.vm_max_threads != 0)
        zmalloc_enable_thread_safeness(); /* we need thread safe zmalloc() */

    expandVmSwapFilename();
    
    server.vm_fp = fopen(server.vm_swap_file,"wr+b");

    server.vm_fd = fileno(server.vm_fp);
    server.vm_next_page = 0;
    server.vm_near_pages = 0;
    server.vm_stats_used_pages = 0;
    server.vm_stats_swapped_objects = 0;
    server.vm_stats_swapouts = 0;
    server.vm_stats_swapins = 0;
    totsize = server.vm_pages*server.vm_page_size;

    ftruncate(server.vm_fd,totsize) ;

    server.vm_bitmap = zmalloc((server.vm_pages+7)/8);
    memset(server.vm_bitmap,0,(server.vm_pages+7)/8);

    /* Initialize threaded I/O (used by Virtual Memory) */
    server.io_newjobs = listCreate();
    server.io_processing = listCreate();
    server.io_processed = listCreate();
    server.io_ready_clients = listCreate();
    pthread_mutex_init(&server.io_mutex,NULL);
    pthread_mutex_init(&server.obj_freelist_mutex,NULL);
    pthread_mutex_init(&server.io_swapfile_mutex,NULL);
    server.io_active_threads = 0;

    server.io_ready_pipe_read = pipefds[0];
    server.io_ready_pipe_write = pipefds[1];
    redisAssert(anetNonBlock(NULL,server.io_ready_pipe_read) != ANET_ERR);
    /* LZF requires a lot of stack */
    pthread_attr_init(&server.io_threads_attr);
    pthread_attr_getstacksize(&server.io_threads_attr, &stacksize);
    while (stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&server.io_threads_attr, stacksize);
    /* Listen for events in the threaded I/O pipe */
    aeCreateFileEvent(server.el, server.io_ready_pipe_read, AE_READABLE,
        vmThreadedIOCompletedJob, NULL);
}

/* Mark the page as used */
static void vmMarkPageUsed(off_t page) {
    off_t byte = page/8;
    int bit = page&7;
    redisAssert(vmFreePage(page) == 1);
    server.vm_bitmap[byte] |= 1<<bit;
}

/* Mark N contiguous pages as used, with 'page' being the first. */
static void vmMarkPagesUsed(off_t page, off_t count) {
    off_t j;

    for (j = 0; j < count; j++)
        vmMarkPageUsed(page+j);
    server.vm_stats_used_pages += count;
}

/* Mark the page as free */
static void vmMarkPageFree(off_t page) {
    off_t byte = page/8;
    int bit = page&7;
    redisAssert(vmFreePage(page) == 0);
    server.vm_bitmap[byte] &= ~(1<<bit);
}

/* Mark N contiguous pages as free, with 'page' being the first. */
static void vmMarkPagesFree(off_t page, off_t count) {
    off_t j;

    for (j = 0; j < count; j++)
        vmMarkPageFree(page+j);
    server.vm_stats_used_pages -= count;
    redisLog(REDIS_DEBUG,"Mark FREE pages: %lld pages at %lld\n",
        (long long)count, (long long)page);
}

/* Test if the page is free */
static int vmFreePage(off_t page) {
    off_t byte = page/8;
    int bit = page&7;
    return (server.vm_bitmap[byte] & (1<<bit)) == 0;
}

/* Find N contiguous free pages storing the first page of the cluster in *first.
 * Returns REDIS_OK if it was able to find N contiguous pages, otherwise 
 * REDIS_ERR is returned.
 *
 * This function uses a simple algorithm: we try to allocate
 * REDIS_VM_MAX_NEAR_PAGES sequentially, when we reach this limit we start
 * again from the start of the swap file searching for free spaces.
 *
 * If it looks pretty clear that there are no free pages near our offset
 * we try to find less populated places doing a forward jump of
 * REDIS_VM_MAX_RANDOM_JUMP, then we start scanning again a few pages
 * without hurry, and then we jump again and so forth...
 * 
 * This function can be improved using a free list to avoid to guess
 * too much, since we could collect data about freed pages.
 *
 * note: I implemented this function just after watching an episode of
 * Battlestar Galactica, where the hybrid was continuing to say "JUMP!"
 */
static int vmFindContiguousPages(off_t *first, off_t n) {
    off_t base, offset = 0, since_jump = 0, numfree = 0;

    if (server.vm_near_pages == REDIS_VM_MAX_NEAR_PAGES) {
        server.vm_near_pages = 0;
        server.vm_next_page = 0;
    }
    server.vm_near_pages++; /* Yet another try for pages near to the old ones */
    base = server.vm_next_page;

    while(offset < server.vm_pages) {
        off_t this = base+offset;

        /* If we overflow, restart from page zero */
        if (this >= server.vm_pages) {
            this -= server.vm_pages;
            if (this == 0) {
                /* Just overflowed, what we found on tail is no longer
                 * interesting, as it's no longer contiguous. */
                numfree = 0;
            }
        }

        if (vmFreePage(this)) {
            /* This is a free page */
            numfree++;
            /* Already got N free pages? Return to the caller, with success */
            if (numfree == n) {
                *first = this-(n-1);
                server.vm_next_page = this+1;

                return REDIS_OK;
            }
        } else {
            /* The current one is not a free page */
            numfree = 0;
        }

        /* Fast-forward if the current page is not free and we already
         * searched enough near this place. */
        since_jump++;
        if (!numfree && since_jump >= REDIS_VM_MAX_RANDOM_JUMP/4) {
            offset += random() % REDIS_VM_MAX_RANDOM_JUMP;
            since_jump = 0;
            /* Note that even if we rewind after the jump, we are don't need
             * to make sure numfree is set to zero as we only jump *if* it
             * is set to zero. */
        } else {
            /* Otherwise just check the next page */
            offset++;
        }
    }
    return REDIS_ERR;
}

/* Write the specified object at the specified page of the swap file */
static int vmWriteObjectOnSwap(robj *o, off_t page) {
    if (server.vm_enabled) pthread_mutex_lock(&server.io_swapfile_mutex);
    fseeko(server.vm_fp,page*server.vm_page_size,SEEK_SET) ;

    rdbSaveObject(server.vm_fp,o);
    fflush(server.vm_fp);
    if (server.vm_enabled) pthread_mutex_unlock(&server.io_swapfile_mutex);
    return REDIS_OK;
}

/* Swap the 'val' object relative to 'key' into disk. Store all the information
 * needed to later retrieve the object into the key object.
 * If we can't find enough contiguous empty pages to swap the object on disk
 * REDIS_ERR is returned. */
static int vmSwapObjectBlocking(robj *key, robj *val) {
    off_t pages = rdbSavedObjectPages(val,NULL);
    off_t page;

    assert(key->storage == REDIS_VM_MEMORY);
    assert(key->refcount == 1);
    if (vmFindContiguousPages(&page,pages) == REDIS_ERR) return REDIS_ERR;
    if (vmWriteObjectOnSwap(val,page) == REDIS_ERR) return REDIS_ERR;
    key->vm.page = page;
    key->vm.usedpages = pages;
    key->storage = REDIS_VM_SWAPPED;
    key->vtype = val->type;
    decrRefCount(val); /* Deallocate the object from memory. */
    vmMarkPagesUsed(page,pages);

    server.vm_stats_swapped_objects++;
    server.vm_stats_swapouts++;
    return REDIS_OK;
}

static robj *vmReadObjectFromSwap(off_t page, int type) {
    if (server.vm_enabled) pthread_mutex_lock(&server.io_swapfile_mutex);

    fseeko(server.vm_fp,page*server.vm_page_size,SEEK_SET);
    robj *o = rdbLoadObject(type,server.vm_fp);

    if (server.vm_enabled) pthread_mutex_unlock(&server.io_swapfile_mutex);
    return o;
}

/* Load the value object relative to the 'key' object from swap to memory.
 * The newly allocated object is returned.
 *
 * If preview is true the unserialized object is returned to the caller but
 * no changes are made to the key object, nor the pages are marked as freed */
static robj *vmGenericLoadObject(robj *key, int preview) {
    robj *val;

    redisAssert(key->storage == REDIS_VM_SWAPPED || key->storage == REDIS_VM_LOADING);
    val = vmReadObjectFromSwap(key->vm.page,key->vtype);
    if (!preview) {
        key->storage = REDIS_VM_MEMORY;
        key->vm.atime = server.unixtime;
        vmMarkPagesFree(key->vm.page,key->vm.usedpages);

        server.vm_stats_swapped_objects--;
    } 

    server.vm_stats_swapins++;
    return val;
}

/* Plain object loading, from swap to memory */
static robj *vmLoadObject(robj *key) {
    /* If we are loading the object in background, stop it, we
     * need to load this object synchronously ASAP. */
    if (key->storage == REDIS_VM_LOADING)
        vmCancelThreadedIOJob(key);
    return vmGenericLoadObject(key,0);
}

/* Just load the value on disk, without to modify the key.
 * This is useful when we want to perform some operation on the value
 * without to really bring it from swap to memory, like while saving the
 * dataset or rewriting the append only log. */
static robj *vmPreviewObject(robj *key) {
    return vmGenericLoadObject(key,1);
}

/* How a good candidate is this object for swapping?
 * The better candidate it is, the greater the returned value.
 *
 * Currently we try to perform a fast estimation of the object size in
 * memory, and combine it with aging informations.
 *
 * Basically swappability = idle-time * log(estimated size)
 *
 * Bigger objects are preferred over smaller objects, but not
 * proportionally, this is why we use the logarithm. This algorithm is
 * just a first try and will probably be tuned later. */
static double computeObjectSwappability(robj *o) {
    time_t age = server.unixtime - o->vm.atime;
    long asize = 0;
    list *l;
    dict *d;
    struct dictEntry *de;
    int z;

    if (age <= 0) return 0;
    switch(o->type) {
    case REDIS_STRING:
        if (o->encoding != REDIS_ENCODING_RAW) {
            asize = sizeof(*o);
        } else {
            asize = sdslen(o->ptr)+sizeof(*o)+sizeof(long)*2;
        }
        break;
    case REDIS_LIST:
        l = o->ptr;
        listNode *ln = listFirst(l);

        asize = sizeof(list);
        if (ln) {
            robj *ele = ln->value;
            long elesize;

            elesize = (ele->encoding == REDIS_ENCODING_RAW) ?
                            (sizeof(*o)+sdslen(ele->ptr)) :
                            sizeof(*o);
            asize += (sizeof(listNode)+elesize)*listLength(l);
        }
        break;
    case REDIS_SET:
    case REDIS_ZSET:
        z = (o->type == REDIS_ZSET);
        d = z ? ((zset*)o->ptr)->dict : o->ptr;

        asize = sizeof(dict)+(sizeof(struct dictEntry*)*dictSlots(d));
        if (z) asize += sizeof(zset)-sizeof(dict);
        if (dictSize(d)) {
            long elesize;
            robj *ele;

            de = dictGetRandomKey(d);
            ele = dictGetEntryKey(de);
            elesize = (ele->encoding == REDIS_ENCODING_RAW) ?
                            (sizeof(*o)+sdslen(ele->ptr)) :
                            sizeof(*o);
            asize += (sizeof(struct dictEntry)+elesize)*dictSize(d);
            if (z) asize += sizeof(zskiplistNode)*dictSize(d);
        }
        break;
    case REDIS_HASH:
        if (o->encoding == REDIS_ENCODING_ZIPMAP) {
            unsigned char *p = zipmapRewind((unsigned char*)o->ptr);
            unsigned int len = zipmapLen((unsigned char*)o->ptr);
            unsigned int klen, vlen;
            unsigned char *key, *val;

            if ((p = zipmapNext(p,&key,&klen,&val,&vlen)) == NULL) {
                klen = 0;
                vlen = 0;
            }
            asize = len*(klen+vlen+3);
        } else if (o->encoding == REDIS_ENCODING_HT) {
            d = o->ptr;
            asize = sizeof(dict)+(sizeof(struct dictEntry*)*dictSlots(d));
            if (dictSize(d)) {
                long elesize;
                robj *ele;

                de = dictGetRandomKey(d);
                ele = dictGetEntryKey(de);
                elesize = (ele->encoding == REDIS_ENCODING_RAW) ?
                                (sizeof(*o)+sdslen(ele->ptr)) :
                                sizeof(*o);
                ele = dictGetEntryVal(de);
                elesize = (ele->encoding == REDIS_ENCODING_RAW) ?
                                (sizeof(*o)+sdslen(ele->ptr)) :
                                sizeof(*o);
                asize += (sizeof(struct dictEntry)+elesize)*dictSize(d);
            }
        }
        break;
    }
    return (double)age*log(1+asize);
}

/* Try to swap an object that's a good candidate for swapping.
 * Returns REDIS_OK if the object was swapped, REDIS_ERR if it's not possible
 * to swap any object at all.
 *
 * If 'usethreaded' is true, Redis will try to swap the object in background
 * using I/O threads. */
static int vmSwapOneObject(int usethreads) {
    int j, i;
    struct dictEntry *best = NULL;
    double best_swappability = 0;
    redisDb *best_db = NULL;
    robj *key, *val;

    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        /* Why maxtries is set to 100?
         * Because this way (usually) we'll find 1 object even if just 1% - 2%
         * are swappable objects */
        int maxtries = 100;

        if (dictSize(db->dict) == 0) continue;
        for (i = 0; i < 5; i++) {
            dictEntry *de;
            double swappability;

            if (maxtries) maxtries--;
            de = dictGetRandomKey(db->dict);
            key = dictGetEntryKey(de);
            val = dictGetEntryVal(de);
            /* Only swap objects that are currently in memory.
             *
             * Also don't swap shared objects if threaded VM is on, as we
             * try to ensure that the main thread does not touch the
             * object while the I/O thread is using it, but we can't
             * control other keys without adding additional mutex. */
            if (key->storage != REDIS_VM_MEMORY ||
                (server.vm_max_threads != 0 && val->refcount != 1)) {
                if (maxtries) i--; /* don't count this try */
                continue;
            }
            swappability = computeObjectSwappability(val);
            if (!best || swappability > best_swappability) {
                best = de;
                best_swappability = swappability;
                best_db = db;
            }
        }
    }
    if (best == NULL) return REDIS_ERR;
    key = dictGetEntryKey(best);
    val = dictGetEntryVal(best);

    redisLog(REDIS_DEBUG,"Key with best swappability: %s, %f",
        key->ptr, best_swappability);

    /* Unshare the key if needed */
    if (key->refcount > 1) {
        robj *newkey = dupStringObject(key);
        decrRefCount(key);
        key = dictGetEntryKey(best) = newkey;
    }
    /* Swap it */
    if (usethreads) {
        vmSwapObjectThreaded(key,val,best_db);
        return REDIS_OK;
    } else {
        if (vmSwapObjectBlocking(key,val) == REDIS_OK) {
            dictGetEntryVal(best) = NULL;
            return REDIS_OK;
        } else {
            return REDIS_ERR;
        }
    }
}

static int vmSwapOneObjectBlocking() {
    return vmSwapOneObject(0);
}

static int vmSwapOneObjectThreaded() {
    return vmSwapOneObject(1);
}

/* Return true if it's safe to swap out objects in a given moment.
 * Basically we don't want to swap objects out while there is a BGSAVE
 * or a BGAEOREWRITE running in backgroud. */
static int vmCanSwapOut(void) {
    return (server.bgsavechildpid == -1 && server.bgrewritechildpid == -1);
}

/* Delete a key if swapped. Returns 1 if the key was found, was swapped
 * and was deleted. Otherwise 0 is returned. */
static int deleteIfSwapped(redisDb *db, robj *key) {
    dictEntry *de;
    robj *foundkey;

    if ((de = dictFind(db->dict,key)) == NULL) return 0;
    foundkey = dictGetEntryKey(de);
    if (foundkey->storage == REDIS_VM_MEMORY) return 0;
    deleteKey(db,key);
    return 1;
}





/* =================== Virtual Memory - Threaded I/O  ======================= */

static void freeIOJob(iojob *j) {
    if ((j->type == REDIS_IOJOB_PREPARE_SWAP ||
        j->type == REDIS_IOJOB_DO_SWAP ||
        j->type == REDIS_IOJOB_LOAD) && j->val != NULL)
        decrRefCount(j->val);
    decrRefCount(j->key);
    zfree(j);
}

/* Every time a thread finished a Job, it writes a byte into the write side
 * of an unix pipe in order to "awake" the main thread, and this function
 * is called. */
static void vmThreadedIOCompletedJob(aeEventLoop *el, int fd, void *privdata,
            int mask)
{
    char buf[1];
    int retval, processed = 0, toprocess = -1, trytoswap = 1;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    /* For every byte we read in the read side of the pipe, there is one
     * I/O job completed to process. */
    while((retval = read(fd,buf,1)) == 1) {
        iojob *j;
        listNode *ln;
        robj *key;
        struct dictEntry *de;

        redisLog(REDIS_DEBUG,"Processing I/O completed job");

        /* Get the processed element (the oldest one) */
        lockThreadedIO();
        assert(listLength(server.io_processed) != 0);
        if (toprocess == -1) {
            toprocess = (listLength(server.io_processed)*REDIS_MAX_COMPLETED_JOBS_PROCESSED)/100;
            if (toprocess <= 0) toprocess = 1;
        }
        ln = listFirst(server.io_processed);
        j = ln->value;
        listDelNode(server.io_processed,ln);
        unlockThreadedIO();
        /* If this job is marked as canceled, just ignore it */
        if (j->canceled) {
            freeIOJob(j);
            continue;
        }
        /* Post process it in the main thread, as there are things we
         * can do just here to avoid race conditions and/or invasive locks */
        redisLog(REDIS_DEBUG,"Job %p type: %d, key at %p (%s) refcount: %d\n", (void*) j, j->type, (void*)j->key, (char*)j->key->ptr, j->key->refcount);
        de = dictFind(j->db->dict,j->key);
        assert(de != NULL);
        key = dictGetEntryKey(de);
        if (j->type == REDIS_IOJOB_LOAD) {
            redisDb *db;

            /* Key loaded, bring it at home */
            key->storage = REDIS_VM_MEMORY;
            key->vm.atime = server.unixtime;
            vmMarkPagesFree(key->vm.page,key->vm.usedpages);
            redisLog(REDIS_DEBUG, "VM: object %s loaded from disk (threaded)",
                (unsigned char*) key->ptr);
            server.vm_stats_swapped_objects--;
            server.vm_stats_swapins++;
            dictGetEntryVal(de) = j->val;
            incrRefCount(j->val);
            db = j->db;
            freeIOJob(j);
            /* Handle clients waiting for this key to be loaded. */
            handleClientsBlockedOnSwappedKey(db,key);
        } else if (j->type == REDIS_IOJOB_PREPARE_SWAP) {
            /* Now we know the amount of pages required to swap this object.
             * Let's find some space for it, and queue this task again
             * rebranded as REDIS_IOJOB_DO_SWAP. */
            if (!vmCanSwapOut() ||
                vmFindContiguousPages(&j->page,j->pages) == REDIS_ERR)
            {
                /* Ooops... no space or we can't swap as there is
                 * a fork()ed Redis trying to save stuff on disk. */
                freeIOJob(j);
                key->storage = REDIS_VM_MEMORY; /* undo operation */
            } else {
                /* Note that we need to mark this pages as used now,
                 * if the job will be canceled, we'll mark them as freed
                 * again. */
                vmMarkPagesUsed(j->page,j->pages);
                j->type = REDIS_IOJOB_DO_SWAP;
                lockThreadedIO();
                queueIOJob(j);
                unlockThreadedIO();
            }
        } else if (j->type == REDIS_IOJOB_DO_SWAP) {
            robj *val;

            /* Key swapped. We can finally free some memory. */
            if (key->storage != REDIS_VM_SWAPPING) {
                printf("key->storage: %d\n",key->storage);
                printf("key->name: %s\n",(char*)key->ptr);
                printf("key->refcount: %d\n",key->refcount);
                printf("val: %p\n",(void*)j->val);
                printf("val->type: %d\n",j->val->type);
                printf("val->ptr: %s\n",(char*)j->val->ptr);
            }
            redisAssert(key->storage == REDIS_VM_SWAPPING);
            val = dictGetEntryVal(de);
            key->vm.page = j->page;
            key->vm.usedpages = j->pages;
            key->storage = REDIS_VM_SWAPPED;
            key->vtype = j->val->type;
            decrRefCount(val); /* Deallocate the object from memory. */
            dictGetEntryVal(de) = NULL;
            redisLog(REDIS_DEBUG,
                "VM: object %s swapped out at %lld (%lld pages) (threaded)",
                (unsigned char*) key->ptr,
                (unsigned long long) j->page, (unsigned long long) j->pages);
            server.vm_stats_swapped_objects++;
            server.vm_stats_swapouts++;
            freeIOJob(j);
            /* Put a few more swap requests in queue if we are still
             * out of memory */
            if (trytoswap && vmCanSwapOut() &&
                zmalloc_used_memory() > server.vm_max_memory)
            {
                int more = 1;
                while(more) {
                    lockThreadedIO();
                    more = listLength(server.io_newjobs) <
                            (unsigned) server.vm_max_threads;
                    unlockThreadedIO();
                    /* Don't waste CPU time if swappable objects are rare. */
                    if (vmSwapOneObjectThreaded() == REDIS_ERR) {
                        trytoswap = 0;
                        break;
                    }
                }
            }
        }
        processed++;
        if (processed == toprocess) return;
    }
    if (retval < 0 && errno != EAGAIN) {
        redisLog(REDIS_WARNING,
            "WARNING: read(2) error in vmThreadedIOCompletedJob() %s",
            strerror(errno));
    }
}

static void lockThreadedIO(void) {
    pthread_mutex_lock(&server.io_mutex);
}

static void unlockThreadedIO(void) {
    pthread_mutex_unlock(&server.io_mutex);
}

/* Remove the specified object from the threaded I/O queue if still not
 * processed, otherwise make sure to flag it as canceled. */
static void vmCancelThreadedIOJob(robj *o) {
    list *lists[3] = {
        server.io_newjobs,      /* 0 */
        server.io_processing,   /* 1 */
        server.io_processed     /* 2 */
    };
    int i;

    assert(o->storage == REDIS_VM_LOADING || o->storage == REDIS_VM_SWAPPING);
again:
    lockThreadedIO();
    /* Search for a matching key in one of the queues */
    for (i = 0; i < 3; i++) {
        listNode *ln;
        listIter li;

        listRewind(lists[i],&li);
        while ((ln = listNext(&li)) != NULL) {
            iojob *job = ln->value;

            if (job->canceled) continue; /* Skip this, already canceled. */
            if (compareStringObjects(job->key,o) == 0) {
                redisLog(REDIS_DEBUG,"*** CANCELED %p (%s) (type %d) (LIST ID %d)\n",
                    (void*)job, (char*)o->ptr, job->type, i);
                /* Mark the pages as free since the swap didn't happened
                 * or happened but is now discarded. */
                if (i != 1 && job->type == REDIS_IOJOB_DO_SWAP)
                    vmMarkPagesFree(job->page,job->pages);
                /* Cancel the job. It depends on the list the job is
                 * living in. */
                switch(i) {
                case 0: /* io_newjobs */
                    /* If the job was yet not processed the best thing to do
                     * is to remove it from the queue at all */
                    freeIOJob(job);
                    listDelNode(lists[i],ln);
                    break;
                case 1: /* io_processing */
                    /* Oh Shi- the thread is messing with the Job:
                     *
                     * Probably it's accessing the object if this is a
                     * PREPARE_SWAP or DO_SWAP job.
                     * If it's a LOAD job it may be reading from disk and
                     * if we don't wait for the job to terminate before to
                     * cancel it, maybe in a few microseconds data can be
                     * corrupted in this pages. So the short story is:
                     *
                     * Better to wait for the job to move into the
                     * next queue (processed)... */

                    /* We try again and again until the job is completed. */
                    unlockThreadedIO();
                    /* But let's wait some time for the I/O thread
                     * to finish with this job. After all this condition
                     * should be very rare. */
                    usleep(1);
                    goto again;
                case 2: /* io_processed */
                    /* The job was already processed, that's easy...
                     * just mark it as canceled so that we'll ignore it
                     * when processing completed jobs. */
                    job->canceled = 1;
                    break;
                }
                /* Finally we have to adjust the storage type of the object
                 * in order to "UNDO" the operaiton. */
                if (o->storage == REDIS_VM_LOADING)
                    o->storage = REDIS_VM_SWAPPED;
                else if (o->storage == REDIS_VM_SWAPPING)
                    o->storage = REDIS_VM_MEMORY;
                unlockThreadedIO();
                return;
            }
        }
    }
    unlockThreadedIO();
    assert(1 != 1); /* We should never reach this */
}

static void *IOThreadEntryPoint(void *arg) {
    iojob *j;
    listNode *ln;
    REDIS_NOTUSED(arg);

    pthread_detach(pthread_self());
    while(1) {
        /* Get a new job to process */
        lockThreadedIO();
        if (listLength(server.io_newjobs) == 0) {
            /* No new jobs in queue, exit. */
            redisLog(REDIS_DEBUG,"Thread %ld exiting, nothing to do",
                (long) pthread_self());
            server.io_active_threads--;
            unlockThreadedIO();
            return NULL;
        }
        ln = listFirst(server.io_newjobs);
        j = ln->value;
        listDelNode(server.io_newjobs,ln);
        /* Add the job in the processing queue */
        j->thread = pthread_self();
        listAddNodeTail(server.io_processing,j);
        ln = listLast(server.io_processing); /* We use ln later to remove it */
        unlockThreadedIO();
        redisLog(REDIS_DEBUG,"Thread %ld got a new job (type %d): %p about key '%s'",
            (long) pthread_self(), j->type, (void*)j, (char*)j->key->ptr);

        /* Process the Job */
        if (j->type == REDIS_IOJOB_LOAD) {
            j->val = vmReadObjectFromSwap(j->page,j->key->vtype);
        } else if (j->type == REDIS_IOJOB_PREPARE_SWAP) {
            FILE *fp = fopen("/dev/null","w+");
            j->pages = rdbSavedObjectPages(j->val,fp);
            fclose(fp);
        } else if (j->type == REDIS_IOJOB_DO_SWAP) {
            if (vmWriteObjectOnSwap(j->val,j->page) == REDIS_ERR)
                j->canceled = 1;
        }

        /* Done: insert the job into the processed queue */
        redisLog(REDIS_DEBUG,"Thread %ld completed the job: %p (key %s)",
            (long) pthread_self(), (void*)j, (char*)j->key->ptr);
        lockThreadedIO();
        listDelNode(server.io_processing,ln);
        listAddNodeTail(server.io_processed,j);
        unlockThreadedIO();
        
        /* Signal the main thread there is new stuff to process */
        assert(write(server.io_ready_pipe_write,"x",1) == 1);
    }
    return NULL; /* never reached */
}

static void spawnIOThread(void) {
    pthread_t thread;
    sigset_t mask, omask;
    int err;

    sigemptyset(&mask);
    sigaddset(&mask,SIGCHLD);
    sigaddset(&mask,SIGHUP);
    sigaddset(&mask,SIGPIPE);
    pthread_sigmask(SIG_SETMASK, &mask, &omask);
    while ((err = pthread_create(&thread,&server.io_threads_attr,IOThreadEntryPoint,NULL)) != 0) {
        redisLog(REDIS_WARNING,"Unable to spawn an I/O thread: %s",
            strerror(err));
        usleep(1000000);
    }
    pthread_sigmask(SIG_SETMASK, &omask, NULL);
    server.io_active_threads++;
}

/* We need to wait for the last thread to exit before we are able to
 * fork() in order to BGSAVE or BGREWRITEAOF. */
static void waitEmptyIOJobsQueue(void) {
    while(1) {
        int io_processed_len;

        lockThreadedIO();
        if (listLength(server.io_newjobs) == 0 &&
            listLength(server.io_processing) == 0 &&
            server.io_active_threads == 0)
        {
            unlockThreadedIO();
            return;
        }
        /* While waiting for empty jobs queue condition we post-process some
         * finshed job, as I/O threads may be hanging trying to write against
         * the io_ready_pipe_write FD but there are so much pending jobs that
         * it's blocking. */
        io_processed_len = listLength(server.io_processed);
        unlockThreadedIO();
        if (io_processed_len) {
            vmThreadedIOCompletedJob(NULL,server.io_ready_pipe_read,NULL,0);
            usleep(1000); /* 1 millisecond */
        } else {
            usleep(10000); /* 10 milliseconds */
        }
    }
}

static void vmReopenSwapFile(void) {
    /* Note: we don't close the old one as we are in the child process
     * and don't want to mess at all with the original file object. */
    server.vm_fp = fopen(server.vm_swap_file,"r+b");
    if (server.vm_fp == NULL) {
        redisLog(REDIS_WARNING,"Can't re-open the VM swap file: %s. Exiting.",
            server.vm_swap_file);
        _exit(1);
    }
    server.vm_fd = fileno(server.vm_fp);
}

/* This function must be called while with threaded IO locked */
static void queueIOJob(iojob *j) {
    redisLog(REDIS_DEBUG,"Queued IO Job %p type %d about key '%s'\n",
        (void*)j, j->type, (char*)j->key->ptr);
    listAddNodeTail(server.io_newjobs,j);
    if (server.io_active_threads < server.vm_max_threads)
        spawnIOThread();
}

static int vmSwapObjectThreaded(robj *key, robj *val, redisDb *db) {
    iojob *j;
    
    assert(key->storage == REDIS_VM_MEMORY);
    assert(key->refcount == 1);

    j = zmalloc(sizeof(*j));
    j->type = REDIS_IOJOB_PREPARE_SWAP;
    j->db = db;
    j->key = dupStringObject(key);
    j->val = val;
    incrRefCount(val);
    j->canceled = 0;
    j->thread = (pthread_t) -1;
    key->storage = REDIS_VM_SWAPPING;

    lockThreadedIO();
    queueIOJob(j);
    unlockThreadedIO();
    return REDIS_OK;
}



/* ============ Virtual Memory - Blocking clients on missing keys =========== */

/* This function makes the clinet 'c' waiting for the key 'key' to be loaded.
 * If there is not already a job loading the key, it is craeted.
 * The key is added to the io_keys list in the client structure, and also
 * in the hash table mapping swapped keys to waiting clients, that is,
 * server.io_waited_keys. */
static int waitForSwappedKey(redisClient *c, robj *key) {
    struct dictEntry *de;
    robj *o;
    list *l;

    /* If the key does not exist or is already in RAM we don't need to
     * block the client at all. */
    de = dictFind(c->db->dict,key);
    if (de == NULL) return 0;
    o = dictGetEntryKey(de);
    if (o->storage == REDIS_VM_MEMORY) {
        return 0;
    } else if (o->storage == REDIS_VM_SWAPPING) {
        /* We were swapping the key, undo it! */
        vmCancelThreadedIOJob(o);
        return 0;
    }
    
    /* OK: the key is either swapped, or being loaded just now. */

    /* Add the key to the list of keys this client is waiting for.
     * This maps clients to keys they are waiting for. */
    listAddNodeTail(c->io_keys,key);
    incrRefCount(key);

    /* Add the client to the swapped keys => clients waiting map. */
    de = dictFind(c->db->io_keys,key);
    if (de == NULL) {
        int retval;

        /* For every key we take a list of clients blocked for it */
        l = listCreate();
        retval = dictAdd(c->db->io_keys,key,l);
        incrRefCount(key);
        assert(retval == DICT_OK);
    } else {
        l = dictGetEntryVal(de);
    }
    listAddNodeTail(l,c);

    /* Are we already loading the key from disk? If not create a job */
    if (o->storage == REDIS_VM_SWAPPED) {
        iojob *j;

        o->storage = REDIS_VM_LOADING;
        j = zmalloc(sizeof(*j));
        j->type = REDIS_IOJOB_LOAD;
        j->db = c->db;
        j->key = dupStringObject(key);
        j->key->vtype = o->vtype;
        j->page = o->vm.page;
        j->val = NULL;
        j->canceled = 0;
        j->thread = (pthread_t) -1;
        lockThreadedIO();
        queueIOJob(j);
        unlockThreadedIO();
    }
    return 1;
}

/* Preload keys needed for the ZUNION and ZINTER commands. */
static void zunionInterBlockClientOnSwappedKeys(redisClient *c) {
    int i, num;
    num = atoi(c->argv[2]->ptr);
    for (i = 0; i < num; i++) {
        waitForSwappedKey(c,c->argv[3+i]);
    }
}

/* Is this client attempting to run a command against swapped keys?
 * If so, block it ASAP, load the keys in background, then resume it.
 *
 * The important idea about this function is that it can fail! If keys will
 * still be swapped when the client is resumed, this key lookups will
 * just block loading keys from disk. In practical terms this should only
 * happen with SORT BY command or if there is a bug in this function.
 *
 * Return 1 if the client is marked as blocked, 0 if the client can
 * continue as the keys it is going to access appear to be in memory. */
static int blockClientOnSwappedKeys(struct redisCommand *cmd, redisClient *c) {
    int j, last;

    if (cmd->vm_preload_proc != NULL) {
        cmd->vm_preload_proc(c);
    } else {
        if (cmd->vm_firstkey == 0) return 0;
        last = cmd->vm_lastkey;
        if (last < 0) last = c->argc+last;
        for (j = cmd->vm_firstkey; j <= last; j += cmd->vm_keystep)
            waitForSwappedKey(c,c->argv[j]);
    }

    /* If the client was blocked for at least one key, mark it as blocked. */
    if (listLength(c->io_keys)) {
        c->flags |= REDIS_IO_WAIT;
        aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
        server.vm_blocked_clients++;
        return 1;
    } else {
        return 0;
    }
}

/* Remove the 'key' from the list of blocked keys for a given client.
 *
 * The function returns 1 when there are no longer blocking keys after
 * the current one was removed (and the client can be unblocked). */
static int dontWaitForSwappedKey(redisClient *c, robj *key) {
    list *l;
    listNode *ln;
    listIter li;
    struct dictEntry *de;

    /* Remove the key from the list of keys this client is waiting for. */
    listRewind(c->io_keys,&li);
    while ((ln = listNext(&li)) != NULL) {
        if (compareStringObjects(ln->value,key) == 0) {
            listDelNode(c->io_keys,ln);
            break;
        }
    }
    assert(ln != NULL);

    /* Remove the client form the key => waiting clients map. */
    de = dictFind(c->db->io_keys,key);
    assert(de != NULL);
    l = dictGetEntryVal(de);
    ln = listSearchKey(l,c);
    assert(ln != NULL);
    listDelNode(l,ln);
    if (listLength(l) == 0)
        dictDelete(c->db->io_keys,key);

    return listLength(c->io_keys) == 0;
}

static void handleClientsBlockedOnSwappedKey(redisDb *db, robj *key) {
    struct dictEntry *de;
    list *l;
    listNode *ln;
    int len;

    de = dictFind(db->io_keys,key);
    if (!de) return;

    l = dictGetEntryVal(de);
    len = listLength(l);
    /* Note: we can't use something like while(listLength(l)) as the list
     * can be freed by the calling function when we remove the last element. */
    while (len--) {
        ln = listFirst(l);
        redisClient *c = ln->value;

        if (dontWaitForSwappedKey(c,key)) {
            /* Put the client in the list of clients ready to go as we
             * loaded all the keys about it. */
            listAddNodeTail(server.io_ready_clients,c);
        }
    }
}

