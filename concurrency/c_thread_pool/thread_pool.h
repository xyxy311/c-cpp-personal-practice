#ifndef THREAD_POOL_H
#define THREAD_POOL_H

typedef struct ThreadPool ThreadPool;
typedef void *(*taskFunc)(void *args);  // 任务函数类型的 typedef

// 任务返回值
typedef struct TaskRet
{
    int state;  // 任务是否完成（0：完成，1：未完成，-1：被抛弃）
    void* res;
} TaskRet;


ThreadPool* threadPoolCreate(int min, int max);

void threadPoolDestroy(ThreadPool* thrP);

TaskRet* threadPoolAddTask(ThreadPool* thrP, taskFunc func, void *args);

// 等待线程池中的任务完成
void threadPoolWait(ThreadPool* thrP);

// 返回线程池中工作线程的数量
int threadPoolWorkingThrNum(ThreadPool* thrP);

// 返回线程池中存活线程的数量
int threadPoolLiveThrNum(ThreadPool* thrP);

#endif // THREAD_POOL_H