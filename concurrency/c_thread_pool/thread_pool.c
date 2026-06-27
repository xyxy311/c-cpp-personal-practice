#include "thread_pool.h"
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdatomic.h>

#define atomicRead(x) atomic_load_explicit((x), memory_order_relaxed)
#define atomicWrite(x, v) atomic_store_explicit((x), (v), memory_order_relaxed)
#define atomicInc(x) atomic_fetch_add_explicit((x), 1, memory_order_relaxed)
#define atomicDec(x) atomic_fetch_sub_explicit((x), 1, memory_order_relaxed)



// ================================= 相关结构体 =================================

// 任务队列中的元素
typedef struct Task
{
    taskFunc func;
    void *args;
    TaskRet *ret;
    struct Task *next;
} Task;

// 任务队列
typedef struct TaskQue
{
    Task *front;
    Task *back;
    atomic_int size;
    pthread_mutex_t lk;
    pthread_cond_t notEmpty;    // 任务队列非空
} TaskQue;


// 线程池
typedef struct ThreadPool
{
    // 任务队列
    TaskQue *taskQue;

    // 线程池
    pthread_t thrManager;  // 管理者线程

    int minNum;             // 最小线程数量
    int maxNum;             // 最大线程数量

    atomic_int busyNum;            // 忙的线程的个数
    atomic_int liveNum;            // 存活的线程的个数
    atomic_int exitNum;            // 要销毁的线程个数，可以为负数，但此时相当于等于 0

    pthread_mutex_t mutexPool;  // 锁整个的线程池
    pthread_cond_t allIdle;  // 所有线程都没执行任务
    pthread_cond_t allExit;  // 所有线程都退出了

    atomic_bool shutdown;

} ThreadPool;



// ================================= 函数前置声明 ===============================

static Task* taskCreate(taskFunc func, void *args);
static TaskQue* taskQueCreate();
static void taskQueAddTask(TaskQue *q, Task* task);
static Task* taskQuePullTask(TaskQue *q);
static void taskQueDestroy(TaskQue* q);
static int threadPoolAddThread(ThreadPool* pool);
static void *work(void *args);
static void *manager(void *args);



// ================================= 函数实现 ==================================

static Task* taskCreate(taskFunc func, void *args)
{
    Task* task = malloc(sizeof(Task));
    if (!task) {
        goto errTask;
    }
    task->ret = malloc(sizeof(TaskRet));
    task->ret->state = 1;
    task->ret->res = NULL;
    if (!task->ret) {
        goto cleanTask;
    }
    task->func = func;
    task->args = args;
    task->next = NULL;

    return task;

cleanTask:
    free(task);
errTask:
    return NULL;
}

static TaskQue* taskQueCreate()
{
    TaskQue *q = malloc(sizeof(TaskQue));
    if (q == NULL) {
        return NULL;
    }
    q->front = q->back = NULL;
    atomic_init(&q->size, 0);
    pthread_mutex_init(&q->lk, NULL);
    pthread_cond_init(&q->notEmpty, NULL);

    return q;
}

static void taskQueAddTask(TaskQue *q, Task* task)
{
    pthread_mutex_lock(&q->lk);
    switch(atomicRead(&q->size)) {
    case 0:
        q->front = q->back = task;
        break;
    default:
        q->back->next = task;
        q->back = task;
    }
    atomicInc(&q->size);
    pthread_cond_signal(&q->notEmpty);   // 注意要持锁通知
    pthread_mutex_unlock(&q->lk);
}

static Task* taskQuePullTask(TaskQue *q)
{
    pthread_mutex_lock(&q->lk);
    Task* task = q->front;
    switch(atomicRead(&q->size)) {
    case 0:
        break;
    case 1:
        q->front = q->back = NULL;
        atomicDec(&q->size);
        break;
    default:
        q->front = task->next;
        atomicDec(&q->size);
    }
    pthread_mutex_unlock(&q->lk);

    return task;
}

static void taskQueDestroy(TaskQue* q)
{
    pthread_mutex_lock(&q->lk);
    Task* taski = q->front;
    int size = atomicRead(&q->size);
    for (int i = 0; i < size; ++i) {
        taski->ret->state = -1;  // 任务被抛弃
        Task* temp = taski;
        taski = taski->next;
        free(temp);
    }
    pthread_mutex_unlock(&q->lk);
    pthread_mutex_destroy(&q->lk);
    pthread_cond_destroy(&q->notEmpty);
    free(q);
}


// 创建线程池
ThreadPool *threadPoolCreate(int min, int max)
{
    // 分配线程池内存
    ThreadPool* pool = malloc(sizeof(ThreadPool));   // 分配内存时自动初始化
    if (pool == NULL) {
        goto errPool;
    }

    // 创建队列
    pool->taskQue = taskQueCreate();
    if (!pool->taskQue) {
        goto cleanPool;
    }

    // 初始化锁和条件变量
    pthread_mutex_init(&pool->mutexPool, NULL);
    pthread_cond_init(&pool->allIdle, NULL);
    pthread_cond_init(&pool->allExit, NULL);
    
    // 初始化其它成员
    pool->minNum = min;
    pool->maxNum = max;
    atomic_init(&pool->liveNum, 0);
    atomic_init(&pool->busyNum, 0);
    atomic_init(&pool->exitNum, 0);
    atomic_init(&pool->shutdown, 0);

    // 创建线程
    if (pthread_create(&pool->thrManager, NULL, manager, pool) != 0) {
        goto cleanTaskQ;
    }
    for (int i = 0; i < min; ++i) {
        if (threadPoolAddThread(pool) == -1) { 
            threadPoolDestroy(pool);
            return NULL;
        }
    }
    return pool;

cleanTaskQ:
    taskQueDestroy(pool->taskQue);
cleanPool:
    free(pool);
errPool:
    return NULL;
}

static int threadPoolAddThread(ThreadPool* pool)
{
    pthread_t thr;
    if (pthread_create(&thr, NULL, work, pool) != 0) {
        return -1;  // 失败
    }
    pthread_detach(thr);
    
    return atomicInc(&pool->liveNum);  // 返回原来的线程数量
}


TaskRet* threadPoolAddTask(ThreadPool* thrP, taskFunc func, void *args)
{
    Task* task = taskCreate(func, args);
    if (!task) {
        return NULL;
    }
    taskQueAddTask(thrP->taskQue, task);

    return task->ret;
}


// 等待所有任务完成
void threadPoolWait(ThreadPool* pool)
{
    TaskQue* q = pool->taskQue;
    pthread_mutex_lock(&pool->mutexPool);
    while (atomicRead(&pool->busyNum) != 0 || atomicRead(&q->size) != 0) {
        pthread_cond_wait(&pool->allIdle, &pool->mutexPool);
    }
    pthread_mutex_unlock(&pool->mutexPool);
}


int threadPoolWorkingThrNum(ThreadPool* pool)
{
    return atomicRead(&pool->busyNum);
}

int threadPoolLiveThrNum(ThreadPool* pool)
{
    return atomicRead(&pool->liveNum);
}


// 工作线程
static void *work(void *args) 
{
    ThreadPool *pool = (ThreadPool *) args;

    while (1) {
        // 等待任务队列非空 / 线程池关闭 / 线程数量太多
        TaskQue* q = pool->taskQue;
        pthread_mutex_lock(&q->lk);
        while (atomicRead(&q->size) == 0 && !atomicRead(&pool->shutdown) && atomicRead(&pool->exitNum) <= 0) {
            pthread_cond_wait(&q->notEmpty, &q->lk);
        }
        pthread_mutex_unlock(&q->lk);
        
        // 判断是否需要退出（线程数量太多 / 线程池关闭）
        pthread_mutex_lock(&pool->mutexPool);
        if (atomicRead(&pool->exitNum) > 0 || atomicRead(&pool->shutdown)) {
            break;  // break后会释放锁
        }
        pthread_mutex_unlock(&pool->mutexPool);
   
        // 取任务
        Task* task = taskQuePullTask(q);
        if (!task) {
            continue;
        }
        atomicInc(&pool->busyNum);
        
        // 执行任务
        task->ret->res = task->func(task->args);
        task->ret->state = 0;  // 任务完成
        free(task);  // 注意：用户需要手动释放 TaskRet 结构体

        pthread_mutex_lock(&pool->mutexPool);
        int busyNum = atomicDec(&pool->busyNum);
        if (busyNum <= 1) { // 注意 busyNum 是原来的忙线程数量
            pthread_cond_signal(&pool->allIdle);
        }
        pthread_mutex_unlock(&pool->mutexPool);
 
    }
    // 线程退出
    atomicDec(&pool->exitNum);  // 允许为 exitNum 负数
    int num = atomicDec(&pool->liveNum);
    if (num <= 1) {
        pthread_cond_signal(&pool->allExit);
    }
    pthread_mutex_unlock(&pool->mutexPool);

    return NULL;
}


// 管理者线程
static void *manager(void *args)
{
    ThreadPool *pool = (ThreadPool *) args;
    TaskQue* q = pool->taskQue;

    while (!atomicRead(&pool->shutdown)) {  // 其依据可能过时的状态调整线程数量，但这无伤大雅
        sleep(3);

        // 调整线程数量
        int busyNum = atomicRead(&pool->busyNum);
        int liveNum = atomicRead(&pool->liveNum);
        int exitNum = liveNum - busyNum;
        if (liveNum - exitNum < pool->minNum) {
            exitNum = liveNum - pool->minNum;
        }
        atomic_store_explicit(&pool->exitNum, exitNum, memory_order_relaxed);   // exitNum 可以为负数，此时相当于等于 0 的效果

        pthread_mutex_lock(&q->lk);
        pthread_cond_broadcast(&q->notEmpty);
        pthread_mutex_unlock(&q->lk);

        int taskNum = atomicRead(&q->size);

        // 主动增加线程
        while (taskNum > liveNum && liveNum < pool->maxNum) {
            liveNum = threadPoolAddThread(pool);
            if (liveNum < 0) {  // 添加失败
                break;
            }
            ++liveNum;
        }
    }
    pthread_mutex_lock(&pool->mutexPool);
    while (atomicRead(&pool->liveNum) > 0) {
        pthread_cond_wait(&pool->allExit, &pool->mutexPool);
    }
    pthread_mutex_unlock(&pool->mutexPool);

    return NULL;
}


void threadPoolDestroy(ThreadPool* pool)
{
    TaskQue* q = pool->taskQue;
    // 等待所有线程结束
    atomic_store_explicit(&pool->shutdown, 1, memory_order_relaxed);

    pthread_mutex_lock(&q->lk);
    pthread_cond_broadcast(&q->notEmpty);
    pthread_mutex_unlock(&q->lk);

    pthread_join(pool->thrManager, NULL);  // manager 一定会在所有工作线程退出后退出
    
    taskQueDestroy(pool->taskQue);
    pthread_mutex_destroy(&pool->mutexPool);
    pthread_cond_destroy(&pool->allIdle);
    pthread_cond_destroy(&pool->allExit);
    free(pool);
}