/*
 */

#define REDIS_VERSION "1.3.7"

#include "fmacros.h"
#include "config.h"


#if defined(__sun)
#include "solarisfixes.h"
#endif

#include "redis.h"
#include "ae.h"     /* Event driven programming library */
#include "sds.h"    /* Dynamic safe strings */
#include "anet.h"   /* Networking the easy way */
#include "dict.h"   /* Hash tables */
#include "adlist.h" /* Linked lists */
#include "zmalloc.h" /* total memory usage aware version of malloc/free */
#include "lzf.h"    /* LZF compression library */
#include "pqsort.h" /* Partial qsort for SORT+LIMIT */
#include "zipmap.h"

/* Error codes */
#define REDIS_OK                0
#define REDIS_ERR               -1

/* Static server configuration */
#define REDIS_SERVERPORT        6379    /* TCP port */
#define REDIS_MAXIDLETIME       (60*5)  /* max idle time. default client timeout */
#define REDIS_IOBUF_LEN         1024
#define REDIS_LOADBUF_LEN       1024
#define REDIS_STATIC_ARGS       4
#define REDIS_DEFAULT_DBNUM     16
#define REDIS_CONFIGLINE_MAX    1024
#define REDIS_OBJFREELIST_MAX   1000000 /* Max number of objects to cache */
#define REDIS_MAX_SYNC_TIME     60      /* Slave can't take more to sync */
#define REDIS_EXPIRELOOKUPS_PER_CRON    100 /* try to expire 100 keys/second */
#define REDIS_MAX_WRITE_PER_EVENT (1024*64)
#define REDIS_REQUEST_MAX_SIZE (1024*1024*256) /* max bytes in inline command */

/* If more then REDIS_WRITEV_THRESHOLD write packets are pending use writev */
#define REDIS_WRITEV_THRESHOLD      3
/* Max number of iovecs used for each writev call */
#define REDIS_WRITEV_IOVEC_COUNT    256

/* Hash table parameters */
#define REDIS_HT_MINFILL        10      /* Minimal hash table fill 10% */

/* Command flags */
#define REDIS_CMD_BULK          1       /* Bulk write command */
#define REDIS_CMD_INLINE        2       /* Inline command */
/* REDIS_CMD_DENYOOM reserves a longer comment: all the commands marked with
   this flags will return an error when the 'maxmemory' option is set in the
   config file and the server is using more than maxmemory bytes of memory.
   In short this commands are denied on low memory conditions. */
#define REDIS_CMD_DENYOOM       4

/* Object types */
#define REDIS_STRING    0
#define REDIS_LIST      1
#define REDIS_SET       2
#define REDIS_ZSET      3
#define REDIS_HASH      4

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
#define REDIS_ENCODING_RAW      0   /* Raw representation */
#define REDIS_ENCODING_INT      1   /* Encoded as integer */
#define REDIS_ENCODING_ZIPMAP   2   /* Encoded as zipmap */
#define REDIS_ENCODING_HT       3   /* Encoded as an hash table */

static char* strencoding[] = {
    "raw", 
    "int", 
    "zipmap", 
    "hashtable"
};

/* Object types only used for dumping to disk */
#define REDIS_EXPIRETIME    253
#define REDIS_SELECTDB      254
#define REDIS_EOF           255

/* Defines related to the dump file format. To store 32 bits lengths for short
 * keys requires a lot of space, so we check the most significant 2 bits of
 * the first byte to interpreter the length:
 *
 * 00|000000 => if the two MSB are 00 the len is the 6 bits of this byte
 * 01|000000 00000000 =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
 * 10|000000 [32 bit integer] => if it's 01, a full 32 bit len will follow
 * 11|000000 this means: specially encoded object will follow. The six bits
 *           number specify the kind of object that follows.
 *           See the REDIS_RDB_ENC_* defines.
 *
 * Lenghts up to 63 are stored using a single byte, most DB keys, and may
 * values, will fit inside. */
#define REDIS_RDB_6BITLEN   0
#define REDIS_RDB_14BITLEN  1
#define REDIS_RDB_32BITLEN  2
#define REDIS_RDB_ENCVAL    3
#define REDIS_RDB_LENERR    UINT_MAX

/* When a length of a string object stored on disk has the first two bits
 * set, the remaining two bits specify a special encoding for the object
 * accordingly to the following defines: */
#define REDIS_RDB_ENC_INT8  0   /* 8 bit signed integer */
#define REDIS_RDB_ENC_INT16 1   /* 16 bit signed integer */
#define REDIS_RDB_ENC_INT32 2   /* 32 bit signed integer */
#define REDIS_RDB_ENC_LZF   3   /* string compressed with FASTLZ */

/* Virtual memory object->where field. */
#define REDIS_VM_MEMORY     0      /* The object is on memory */
#define REDIS_VM_SWAPPED    1      /* The object is on disk */
#define REDIS_VM_SWAPPING   2      /* Redis is swapping this object on disk */
#define REDIS_VM_LOADING    3      /* Redis is loading this object from disk */

/* Virtual memory static configuration stuff.
 * Check vmFindContiguousPages() to know more about this magic numbers. */
#define REDIS_VM_MAX_NEAR_PAGES     65536
#define REDIS_VM_MAX_RANDOM_JUMP    4096
#define REDIS_VM_MAX_THREADS        32
#define REDIS_THREAD_STACK_SIZE     (1024*1024*4)

/* The following is the *percentage* of completed I/O jobs to process when the
 * handelr is called. While Virtual Memory I/O operations are performed by
 * threads, this operations must be processed by the main thread when completed
 * in order to take effect. */
#define REDIS_MAX_COMPLETED_JOBS_PROCESSED 1

/* Client flags */
#define REDIS_SLAVE     1     /* This client is a slave server */
#define REDIS_MASTER    2     /* This client is a master server */
#define REDIS_MONITOR   4     /* This client is a slave monitor, see MONITOR */
#define REDIS_MULTI     8     /* This client is in a MULTI context */
#define REDIS_BLOCKED   16    /* The client is waiting in a blocking operation */
#define REDIS_IO_WAIT   32    /* The client is waiting for Virtual Memory I/O */

/* Slave replication state - slave side */
#define REDIS_REPL_NONE         0    /* No active replication */
#define REDIS_REPL_CONNECT      1    /* Must connect to master */
#define REDIS_REPL_CONNECTED    2    /* Connected to master */

/* Slave replication state - from the point of view of master
 * Note that in SEND_BULK and ONLINE state the slave receives new updates
 * in its output queue. In the WAIT_BGSAVE state instead the server is waiting
 * to start the next background saving in order to send updates to it. */
#define REDIS_REPL_WAIT_BGSAVE_START    3 /* master waits bgsave to start feeding it */
#define REDIS_REPL_WAIT_BGSAVE_END      4 /* master waits bgsave to start bulk DB transmission */
#define REDIS_REPL_SEND_BULK            5 /* master is sending the bulk DB */
#define REDIS_REPL_ONLINE               6 /* bulk DB already transmitted, receive updates */

/* List related stuff */
#define REDIS_HEAD 0
#define REDIS_TAIL 1

/* Sort operations */
#define REDIS_SORT_GET      0
#define REDIS_SORT_ASC      1
#define REDIS_SORT_DESC     2
#define REDIS_SORTKEY_MAX   1024

/* Log levels */
#define REDIS_DEBUG         0
#define REDIS_VERBOSE       1
#define REDIS_NOTICE        2
#define REDIS_WARNING       3

/* Anti-warning macro... */
#define REDIS_NOTUSED(V) ((void) V)

#define ZSKIPLIST_MAXLEVEL  32          /* Should be enough for 2^32 elements */
#define ZSKIPLIST_P         0.25        /* Skiplist P = 1/4 */

/* Append only defines */
#define APPENDFSYNC_NO          0
#define APPENDFSYNC_ALWAYS      1
#define APPENDFSYNC_EVERYSEC    2

/* Hashes related defaults */
#define REDIS_HASH_MAX_ZIPMAP_ENTRIES   64
#define REDIS_HASH_MAX_ZIPMAP_VALUE     512

/* We can print the stacktrace, so our assert is defined this way: */
#define redisAssert(_e) ((_e)?(void)0 : (_redisAssert(#_e,__FILE__,__LINE__),_exit(1)))
static void _redisAssert(char *estr, char *file, int line);


/*================================= Data types ============================== */

/* A redis object, that is a type able to hold a string / list / set */

/* The VM object structure */
struct redisObjectVM {
    off_t   page;         /* the page at witch the object is stored on disk */
    off_t   usedpages;    /* number of pages used on disk */
    time_t  atime;        /* Last access time */
} vm;

/* The actual Redis Object */
typedef struct redisObject {
    void *ptr;
    unsigned char type;
    unsigned char encoding;
    unsigned char storage;  /* If this object is a key, where is the value?
                             * REDIS_VM_MEMORY, REDIS_VM_SWAPPED, ... */
    unsigned char vtype; /* If this object is a key, and value is swapped out,
                          * this is the type of the swapped out object. */
    int refcount;
    /* VM fields, this are only allocated if VM is active, otherwise the
     * object allocation function will just allocate
     * sizeof(redisObjct) minus sizeof(redisObjectVM), so using
     * Redis without VM active will not have any overhead. */
    struct redisObjectVM vm;
} robj;

/* Macro used to initalize a Redis object allocated on the stack. */
#define initStaticStringObject(_var, _ptr) \
do { \
    _var.refcount = 1; \
    _var.type = REDIS_STRING; \
    _var.encoding = REDIS_ENCODING_RAW; \
    _var.ptr = _ptr; \
    if (server.vm_enabled) _var.storage = REDIS_VM_MEMORY; \
} while(0);

typedef struct redisDb {
    dict *dict;                 /* The keyspace for this DB */
    dict *expires;              /* Timeout of keys with a timeout set */
    dict *blockingkeys;         /* Keys with clients waiting for data (BLPOP) */
    dict *io_keys;              /* Keys with clients waiting for VM I/O */
    int id;
} redisDb;

/* Client MULTI/EXEC state */
typedef struct multiCmd {
    robj **argv;
    int argc;
    struct redisCommand *cmd;
} multiCmd;

typedef struct multiState {
    multiCmd *commands;     /* Array of MULTI commands */
    int count;              /* Total number of MULTI commands */
} multiState;



/* With multiplexing we need to take per-clinet state.
 * Clients are taken in a liked list. */
typedef struct redisClient {
    int fd;
    redisDb *db;
    int dictid;
    sds querybuf;
    robj **argv, **mbargv;

    int argc, mbargc;
    int bulklen;            /* bulk read len. -1 if not in bulk read mode */
    int multibulk;          /* multi bulk command format active */
    list *reply;
    int sentlen;

    time_t lastinteraction; /* time of the last interaction, used for timeout */
    int flags;              /* REDIS_SLAVE | REDIS_MONITOR | REDIS_MULTI ... */
    int slaveseldb;         /* slave selected db, if this client is a slave */
    int authenticated;      /* when requirepass is non-NULL */
   
    int replstate;          /* replication state if this is a slave */
    int repldbfd;           /* replication DB file descriptor */
    long repldboff;         /* replication DB file offset */
    off_t repldbsize;       /* replication DB file size */
    
    multiState mstate;      /* MULTI/EXEC state */
    robj **blockingkeys;    /* The key we are waiting to terminate a blocking
                             * operation such as BLPOP. Otherwise NULL. */
    int blockingkeysnum;    /* Number of blocking keys */
    time_t blockingto;      /* Blocking operation timeout. If UNIX current time
                             * is >= blockingto then the operation timed out. */
    list *io_keys;          /* Keys this client is waiting to be loaded from the
                             * swap file in order to continue. */
} redisClient;

struct saveparam {
    time_t seconds;
    int changes;
};

/* Global server state structure */
struct redisServer {
    int port;
    int fd;
    redisDb *db;
    dict *sharingpool;          /* Poll used for object sharing */
    unsigned int sharingpoolsize;

    long long dirty;            /* changes to DB from the last save */
    list *clients;
    list *slaves, *monitors;
    char neterr[ANET_ERR_LEN];
    aeEventLoop *el;

    int cronloops;              /* number of times the cron function run */
    list *objfreelist;          /* A list of freed objects to avoid malloc() */
    time_t lastsave;            /* Unix time of last save succeeede */
    /* Fields used only for stats */
    time_t stat_starttime;         /* server start time */
    long long stat_numcommands;    /* number of processed commands */
    long long stat_numconnections; /* number of connections received */

    /* Configuration */
    int verbosity;
    int glueoutputbuf;
    int maxidletime;
    int dbnum;
    int daemonize;

    int appendonly;
    int appendfsync;
    time_t lastfsync;
    int appendfd;
    int appendseldb;
    
    char *pidfile;
    pid_t bgsavechildpid;
    pid_t bgrewritechildpid;
    sds bgrewritebuf; /* buffer taken by parent during oppend only rewrite */
    struct saveparam *saveparams;
    
    int saveparamslen;
    char *logfile;
    char *bindaddr;
    char *dbfilename;
    char *appendfilename;
    char *requirepass;
    
    int shareobjects;
    int rdbcompression;
    
    /* Replication related */
    int isslave;
    char *masterauth;
    char *masterhost;
    int masterport;
    
    redisClient *master;    /* client that is master for this slave */
    int replstate;
    unsigned int maxclients;
    unsigned long long maxmemory;
    unsigned int blpop_blocked_clients;
    unsigned int vm_blocked_clients;
    
    /* Sort parameters - qsort_r() is only available under BSD so we
     * have to take this state global, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
    
    /* Virtual memory configuration */
    int vm_enabled;
    char *vm_swap_file;
    off_t vm_page_size;
    off_t vm_pages;
    unsigned long long vm_max_memory;

    /* Hashes config */
    size_t hash_max_zipmap_entries;
    size_t hash_max_zipmap_value;
    /* Virtual memory state */
    FILE *vm_fp;
    int vm_fd;
    off_t vm_next_page; /* Next probably empty page */
    off_t vm_near_pages; /* Number of pages allocated sequentially */
    
    unsigned char *vm_bitmap; /* Bitmap of free/used pages */
    time_t unixtime;    /* Unix time sampled every second. */
    /* Virtual memory I/O threads stuff */
    /* An I/O thread process an element taken from the io_jobs queue and
     * put the result of the operation in the io_done list. While the
     * job is being processed, it's put on io_processing queue. */
    list *io_newjobs;       /* List of VM I/O jobs yet to be processed */
    list *io_processing;    /* List of VM I/O jobs being processed */
    list *io_processed;     /* List of VM I/O jobs already processed */
    list *io_ready_clients; /* Clients ready to be unblocked. All keys loaded */
    
    pthread_mutex_t io_mutex;           /* lock to access io_jobs/io_done/io_thread_job */
    pthread_mutex_t obj_freelist_mutex; /* safe redis objects creation/free */
    pthread_mutex_t io_swapfile_mutex;  /* So we can lseek + write */
    pthread_attr_t io_threads_attr;     /* attributes for threads creation */
    
    int io_active_threads; /* Number of running I/O threads */
    int vm_max_threads; /* Max number of I/O threads running at the same time */
    /* Our main thread is blocked on the event loop, locking for sockets ready
     * to be read or written, so when a threaded I/O operation is ready to be
     * processed by the main thread, the I/O thread will use a unix pipe to
     * awake the main thread. The followings are the two pipe FDs. */
    int io_ready_pipe_read;
    int io_ready_pipe_write;
    
    /* Virtual memory stats */
    unsigned long long vm_stats_used_pages;
    unsigned long long vm_stats_swapped_objects;
    unsigned long long vm_stats_swapouts;
    unsigned long long vm_stats_swapins;
    FILE *devnull;
};

typedef void redisCommandProc(redisClient *c);
struct redisCommand {
    char *name;
    redisCommandProc *proc;
    int arity;
    int flags;
    /* Use a function to determine which keys need to be loaded
     * in the background prior to executing this command. Takes precedence
     * over vm_firstkey and others, ignored when NULL */
    redisCommandProc *vm_preload_proc;
    /* What keys should be loaded in background when calling this command? */
    int vm_firstkey; /* The first argument that's a key (0 = no keys) */
    int vm_lastkey;  /* THe last argument that's a key */
    int vm_keystep;  /* The step between first and last key */
};

struct redisFunctionSym {
    char *name;
    unsigned long pointer;
};

typedef struct _redisSortObject {
    robj *obj;
    union {
        double score;
        robj *cmpobj;
    } u;
} redisSortObject;

typedef struct _redisSortOperation {
    int type;
    robj *pattern;
} redisSortOperation;

/* ZSETs use a specialized version of Skiplists */

typedef struct zskiplistNode {
    struct zskiplistNode **forward;
    struct zskiplistNode *backward;
    unsigned int *span;
    double score;
    robj *obj;
} zskiplistNode;

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset {
    dict *dict;
    zskiplist *zsl;
} zset;

/* Our shared "common" objects */

struct sharedObjectsStruct {
    robj 
        *crlf, *ok, *err, *emptybulk, *czero, *cone, *pong, *space,
        *colon, *nullbulk, *nullmultibulk, *queued,
        *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,

        *outofrangeerr, *plus,
        *select0, *select1, *select2, *select3, *select4,
        *select5, *select6, *select7, *select8, *select9;
} shared;

/* Global vars that are actally used as constants. The following double
 * values are used for double on-disk serialization, and are initialized
 * at runtime to avoid strange compiler optimizations. */

static double R_Zero, R_PosInf, R_NegInf, R_Nan;

/* VM threaded I/O request message */
#define REDIS_IOJOB_LOAD            0       /* Load from disk to memory */
#define REDIS_IOJOB_PREPARE_SWAP    1       /* Compute needed pages */
#define REDIS_IOJOB_DO_SWAP         2       /* Swap from memory to disk */
typedef struct iojob {
    int type;   /* Request type, REDIS_IOJOB_* */
    redisDb *db;/* Redis database */
    robj *key;  /* This I/O request is about swapping this key */
    robj *val;  /* the value to swap for REDIS_IOREQ_*_SWAP, otherwise this
                 * field is populated by the I/O thread for REDIS_IOREQ_LOAD. */
    off_t page; /* Swap page where to read/write the object */
    off_t pages; /* Swap pages needed to safe object. PREPARE_SWAP return val */
    int canceled; /* True if this command was canceled by blocking side of VM */
    pthread_t thread; /* ID of the thread processing this entry */
} iojob;

/*================================ Prototypes =============================== */

static void freeStringObject(robj *o);
static void freeListObject(robj *o);
static void freeSetObject(robj *o);
static void decrRefCount(void *o);

static robj *createObject(int type, void *ptr);
static void freeClient(redisClient *c);
static int rdbLoad(char *filename);
static void addReply(redisClient *c, robj *obj);
static void addReplySds(redisClient *c, sds s);
static void incrRefCount(robj *o);

static int rdbSaveBackground(char *filename);
static robj *createStringObject(char *ptr, size_t len);
static robj *dupStringObject(robj *o);
static void replicationFeedSlaves(list *slaves, struct redisCommand *cmd, int dictid, robj **argv, int argc);
static void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc);

static int syncWithMaster(void);
static robj *tryObjectSharing(robj *o);
static int tryObjectEncoding(robj *o);
static robj *getDecodedObject(robj *o);
static int removeExpire(redisDb *db, robj *key);
static int expireIfNeeded(redisDb *db, robj *key);

static int deleteIfVolatile(redisDb *db, robj *key);
static int deleteIfSwapped(redisDb *db, robj *key);
static int deleteKey(redisDb *db, robj *key);
static time_t getExpire(redisDb *db, robj *key);
static int setExpire(redisDb *db, robj *key, time_t when);

static void updateSlavesWaitingBgsave(int bgsaveerr);
static void freeMemoryIfNeeded(void);
static int processCommand(redisClient *c);
static void setupSigSegvAction(void);
static void rdbRemoveTempFile(pid_t childpid);
static void aofRemoveTempFile(pid_t childpid);

static size_t stringObjectLen(robj *o);
static void processInputBuffer(redisClient *c);
static zskiplist *zslCreate(void);
static void zslFree(zskiplist *zsl);
static void zslInsert(zskiplist *zsl, double score, robj *obj);

static void sendReplyToClientWritev(aeEventLoop *el, int fd, void *privdata, int mask);
static void initClientMultiState(redisClient *c);
static void freeClientMultiState(redisClient *c);
static void queueMultiCommand(redisClient *c, struct redisCommand *cmd);
static void unblockClientWaitingData(redisClient *c);
static int handleClientsWaitingListPush(redisClient *c, robj *key, robj *ele);

static void vmInit(void);
static void vmMarkPagesFree(off_t page, off_t count);
static robj *vmLoadObject(robj *key);
static robj *vmPreviewObject(robj *key);
static int vmSwapOneObjectBlocking(void);
static int vmSwapOneObjectThreaded(void);
static int vmCanSwapOut(void);

static int tryFreeOneObjectFromFreelist(void);
static void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void vmThreadedIOCompletedJob(aeEventLoop *el, int fd, void *privdata, int mask);
static void vmCancelThreadedIOJob(robj *o);
static void lockThreadedIO(void);
static void unlockThreadedIO(void);

static int vmSwapObjectThreaded(robj *key, robj *val, redisDb *db);
static void freeIOJob(iojob *j);
static void queueIOJob(iojob *j);
static int vmWriteObjectOnSwap(robj *o, off_t page);
static robj *vmReadObjectFromSwap(off_t page, int type);
static void waitEmptyIOJobsQueue(void);
static void vmReopenSwapFile(void);
static int vmFreePage(off_t page);

static void zunionInterBlockClientOnSwappedKeys(redisClient *c);
static int blockClientOnSwappedKeys(struct redisCommand *cmd, redisClient *c);
static int dontWaitForSwappedKey(redisClient *c, robj *key);
static void handleClientsBlockedOnSwappedKey(redisDb *db, robj *key);
static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
static struct redisCommand *lookupCommand(char *name);
static void call(redisClient *c, struct redisCommand *cmd);
static void resetClient(redisClient *c);
static void convertToRealHash(robj *o);

static void authCommand(redisClient *c);
static void pingCommand(redisClient *c);
static void echoCommand(redisClient *c);
static void setCommand(redisClient *c);
static void setnxCommand(redisClient *c);
static void getCommand(redisClient *c);

static void delCommand(redisClient *c);
static void existsCommand(redisClient *c);
static void incrCommand(redisClient *c);
static void decrCommand(redisClient *c);
static void incrbyCommand(redisClient *c);

static void decrbyCommand(redisClient *c);
static void selectCommand(redisClient *c);
static void randomkeyCommand(redisClient *c);
static void keysCommand(redisClient *c);
static void dbsizeCommand(redisClient *c);

static void lastsaveCommand(redisClient *c);
static void saveCommand(redisClient *c);
static void bgsaveCommand(redisClient *c);
static void bgrewriteaofCommand(redisClient *c);
static void shutdownCommand(redisClient *c);

static void moveCommand(redisClient *c);
static void renameCommand(redisClient *c);
static void renamenxCommand(redisClient *c);
static void lpushCommand(redisClient *c);
static void rpushCommand(redisClient *c);

static void lpopCommand(redisClient *c);
static void rpopCommand(redisClient *c);
static void llenCommand(redisClient *c);
static void lindexCommand(redisClient *c);
static void lrangeCommand(redisClient *c);

static void ltrimCommand(redisClient *c);
static void typeCommand(redisClient *c);
static void lsetCommand(redisClient *c);
static void saddCommand(redisClient *c);
static void sremCommand(redisClient *c);

static void smoveCommand(redisClient *c);
static void sismemberCommand(redisClient *c);
static void scardCommand(redisClient *c);
static void spopCommand(redisClient *c);
static void srandmemberCommand(redisClient *c);

static void sinterCommand(redisClient *c);
static void sinterstoreCommand(redisClient *c);
static void sunionCommand(redisClient *c);
static void sunionstoreCommand(redisClient *c);
static void sdiffCommand(redisClient *c);
static void sdiffstoreCommand(redisClient *c);

static void syncCommand(redisClient *c);
static void flushdbCommand(redisClient *c);
static void flushallCommand(redisClient *c);
static void sortCommand(redisClient *c);
static void lremCommand(redisClient *c);
static void rpoplpushcommand(redisClient *c);

static void infoCommand(redisClient *c);
static void mgetCommand(redisClient *c);
static void monitorCommand(redisClient *c);
static void expireCommand(redisClient *c);
static void expireatCommand(redisClient *c);
static void getsetCommand(redisClient *c);

static void ttlCommand(redisClient *c);
static void slaveofCommand(redisClient *c);
static void debugCommand(redisClient *c);
static void msetCommand(redisClient *c);
static void msetnxCommand(redisClient *c);
static void zaddCommand(redisClient *c);

static void zincrbyCommand(redisClient *c);
static void zrangeCommand(redisClient *c);
static void zrangebyscoreCommand(redisClient *c);
static void zcountCommand(redisClient *c);
static void zrevrangeCommand(redisClient *c);
static void zcardCommand(redisClient *c);
static void zremCommand(redisClient *c);
static void zscoreCommand(redisClient *c);
static void zremrangebyscoreCommand(redisClient *c);

static void multiCommand(redisClient *c);
static void execCommand(redisClient *c);
static void discardCommand(redisClient *c);
static void blpopCommand(redisClient *c);
static void brpopCommand(redisClient *c);

static void appendCommand(redisClient *c);
static void substrCommand(redisClient *c);
static void zrankCommand(redisClient *c);
static void zrevrankCommand(redisClient *c);

static void hsetCommand(redisClient *c);
static void hgetCommand(redisClient *c);
static void hdelCommand(redisClient *c);
static void hlenCommand(redisClient *c);

static void zremrangebyrankCommand(redisClient *c);
static void zunionCommand(redisClient *c);
static void zinterCommand(redisClient *c);
static void hkeysCommand(redisClient *c);
static void hvalsCommand(redisClient *c);
static void hgetallCommand(redisClient *c);
static void hexistsCommand(redisClient *c);

/*================================= Globals ================================= */

/* Global vars */
static struct redisServer server; /* server global state */
static struct redisCommand cmdTable[] = {
    {"get",     get,     2,  REDIS_CMD_INLINE,NULL,1,1,1},
    {"set",     set,     3,  REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"setnx",   setnx,   3,  REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"append",  append,  3,  REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"substr",  substr,  4,  REDIS_CMD_INLINE,NULL,1,1,1},
    {"del",     del,    -2,  REDIS_CMD_INLINE,NULL,0,0,0},
    {"exists",  exists,  2,  REDIS_CMD_INLINE,NULL,1,1,1},
    {"incr",    incrCommand,    2,  REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"decr",    decrCommand,    2,  REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"mget",    mgetCommand,   -2,  REDIS_CMD_INLINE,NULL,1,-1,1},
    {"rpush",   rpushCommand,   3,  REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"lpush",   lpushCommand,   3,  REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"rpop",    rpopCommand,    2,  REDIS_CMD_INLINE,NULL,1,1,1},
    {"lpop",    lpopCommand,    2,  REDIS_CMD_INLINE,NULL,1,1,1},
    {"brpop",   brpopCommand,-3,REDIS_CMD_INLINE,NULL,1,1,1},
    {"blpop",   blpopCommand,-3,REDIS_CMD_INLINE,NULL,1,1,1},
    {"llen",    llenCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"lindex",  lindexCommand,3,REDIS_CMD_INLINE,NULL,1,1,1},
    {"lset",    lsetCommand,4,REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"lrange",  lrangeCommand,4,REDIS_CMD_INLINE,NULL,1,1,1},
    {"ltrim",   ltrimCommand,4,REDIS_CMD_INLINE,NULL,1,1,1},
    {"lrem",    lremCommand,4,REDIS_CMD_BULK,NULL,1,1,1},
    {"rpoplpush",rpoplpushcommand,3,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,1,2,1},
    {"sadd",    saddCommand,3,REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"srem",    sremCommand,3,REDIS_CMD_BULK,NULL,1,1,1},
    {"smove",   smoveCommand,4,REDIS_CMD_BULK,NULL,1,2,1},
    {"sismember",sismemberCommand,3,REDIS_CMD_BULK,NULL,1,1,1},
    {"scard",   scardCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"spop",    spopCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"srandmember",srandmemberCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"sinter",  sinterCommand,-2,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,1,-1,1},
    {"sinterstore",sinterstoreCommand,-3,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,2,-1,1},
    {"sunion",  sunionCommand,-2,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,1,-1,1},
    {"sunionstore",sunionstoreCommand,-3,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,2,-1,1},
    {"sdiff",   sdiffCommand,-2,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,1,-1,1},
    {"sdiffstore",sdiffstoreCommand,-3,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,2,-1,1},
    {"smembers",sinterCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"zadd",    zaddCommand,4,REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"zincrby", zincrbyCommand,4,REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"zrem",    zremCommand,3,REDIS_CMD_BULK,NULL,1,1,1},
    {"zremrangebyscore",zremrangebyscoreCommand,4,REDIS_CMD_INLINE,NULL,1,1,1},
    {"zremrangebyrank",zremrangebyrankCommand,4,REDIS_CMD_INLINE,NULL,1,1,1},
    {"zunion",  zunionCommand,-4,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,zunionInterBlockClientOnSwappedKeys,0,0,0},
    {"zinter",  zinterCommand,-4,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,zunionInterBlockClientOnSwappedKeys,0,0,0},
    {"zrange",  zrangeCommand,-4,REDIS_CMD_INLINE,NULL,1,1,1},
    {"zrangebyscore",zrangebyscoreCommand,-4,REDIS_CMD_INLINE,NULL,1,1,1},
    {"zcount",  zcountCommand,4,REDIS_CMD_INLINE,NULL,1,1,1},
    {"zrevrange",zrevrangeCommand,-4,REDIS_CMD_INLINE,NULL,1,1,1},
    {"zcard",   zcardCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"zscore",  zscoreCommand,3,REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"zrank",   zrankCommand,3,REDIS_CMD_BULK,NULL,1,1,1},
    {"zrevrank",zrevrankCommand,3,REDIS_CMD_BULK,NULL,1,1,1},
    {"hset",    hsetCommand,4,REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"hget",    hgetCommand,3,REDIS_CMD_BULK,NULL,1,1,1},
    {"hdel",    hdelCommand,3,REDIS_CMD_BULK,NULL,1,1,1},
    {"hlen",    hlenCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"hkeys",   hkeysCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"hvals",   hvalsCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"hgetall", hgetallCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"hexists", hexistsCommand,3,REDIS_CMD_BULK,NULL,1,1,1},
    {"incrby",  incrbyCommand,3,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"decrby",  decrbyCommand,3,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"getset",  getsetCommand,3,REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"mset",    msetCommand,-3,REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,-1,2},
    {"msetnx",  msetnxCommand,-3,REDIS_CMD_BULK|REDIS_CMD_DENYOOM,NULL,1,-1,2},
    {"randomkey",randomkeyCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"select",  selectCommand,2,REDIS_CMD_INLINE,NULL,0,0,0},
    {"move",    moveCommand,3,REDIS_CMD_INLINE,NULL,1,1,1},
    {"rename",  renameCommand,3,REDIS_CMD_INLINE,NULL,1,1,1},
    {"renamenx",renamenxCommand,3,REDIS_CMD_INLINE,NULL,1,1,1},
    {"expire",  expireCommand,3,REDIS_CMD_INLINE,NULL,0,0,0},
    {"expireat",expireatCommand,3,REDIS_CMD_INLINE,NULL,0,0,0},
    {"keys",    keysCommand,2,REDIS_CMD_INLINE,NULL,0,0,0},
    {"dbsize",  dbsizeCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"auth",    authCommand,2,REDIS_CMD_INLINE,NULL,0,0,0},
    {"ping",    pingCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"echo",    echoCommand,2,REDIS_CMD_BULK,NULL,0,0,0},
    {"save",    saveCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"bgsave",  bgsaveCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"bgrewriteaof",bgrewriteaofCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"shutdown",shutdownCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"lastsave",lastsaveCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"type",    typeCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"multi",   multiCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"exec",    execCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"discard", discardCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"sync",    syncCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"flushdb", flushdbCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"flushall",flushallCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"sort",    sortCommand,-2,REDIS_CMD_INLINE|REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"info",    infoCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"monitor", monitorCommand,1,REDIS_CMD_INLINE,NULL,0,0,0},
    {"ttl",     ttlCommand,2,REDIS_CMD_INLINE,NULL,1,1,1},
    {"slaveof", slaveofCommand,3,REDIS_CMD_INLINE,NULL,0,0,0},
    {"debug",   debugCommand,-2,REDIS_CMD_INLINE,NULL,0,0,0},
    {NULL,NULL,0,0,NULL,0,0,0}
};

/*============================ Utility functions ============================ */

/* Glob-style pattern matching. */
int stringmatchlen(const char *pattern, int patternLen,
        const char *string, int stringLen, int nocase)
{
    // ...
    return 0;
}

static void redisLog(int level, const char *fmt, ...) {
    // ...    
}

/*====================== Hash table type implementation  ==================== */

/* This is an hash table type that uses the SDS dynamic strings libary as
 * keys and radis objects as values (objects can hold SDS strings,
 * lists, sets). */

static void dictVanillaFree(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    zfree(val);
}

static void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

static int sdsDictKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static void dictRedisObjectDestructor(void *privdata, void *val)
{
    decrRefCount(val);
}

static int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return sdsDictKeyCompare(privdata,o1->ptr,o2->ptr);
}

static unsigned int dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

static int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == REDIS_ENCODING_INT &&
        o2->encoding == REDIS_ENCODING_INT &&
        o1->ptr == o2->ptr) return 1;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = sdsDictKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

static unsigned int dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (o->encoding == REDIS_ENCODING_RAW) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == REDIS_ENCODING_INT) {
            char buf[32];
            int len;

            len = snprintf(buf,32,"%ld",(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            unsigned int hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Sets type and expires */
static dictType setDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
static dictType zsetDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    dictVanillaFree            /* val destructor of malloc(sizeof(double)) */
};

/* Db->dict */
static dictType dbDictType = {
    dictObjHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictObjKeyCompare,          /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    dictRedisObjectDestructor   /* val destructor */
};

/* Db->expires */
static dictType keyptrDictType = {
    dictObjHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictObjKeyCompare,         /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

/* Hash type hash table (note that small hashes are represented with zimpaps) */
static dictType hashDictType = {
    dictEncObjHash,             /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictEncObjKeyCompare,       /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    dictRedisObjectDestructor   /* val destructor */
};

/* Keylist hash table type has unencoded redis objects as keys and
 * lists as values. It's used for blocking operations (BLPOP) and to
 * map swapped keys to a list of clients waiting for this keys to be loaded. */
static dictType keylistDictType = {
    dictObjHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictObjKeyCompare,          /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    dictListDestructor          /* val destructor */
};

/* ========================= Random utility functions ======================= */

/* Redis generally does not try to recover from out of memory conditions
 * when allocating objects or strings, it is not clear if it will be possible
 * to report this condition to the client since the networking layer itself
 * is based on heap allocation for send buffers, so we simply abort.
 * At least the code will be simpler to read... */
static void oom(const char *msg) {
    redisLog(REDIS_WARNING, "%s: Out of memory\n",msg);
    sleep(1);
    abort();
}


/* The End */



