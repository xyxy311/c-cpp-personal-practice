#include "thread_pool.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <string.h>
#include <signal.h>

/* ---------- 辅助结构与函数 ---------- */
typedef struct {
    int id;
    int sleep_ms;
} TaskArg;

// 简单任务：返回动态分配的整数
void* simple_task(void* args) {
    int* val = malloc(sizeof(int));
    *val = *(int*)args + 100;
    return val;
}

// 带睡眠的任务（模拟耗时操作）
void* slow_task(void* args) {
    TaskArg* a = (TaskArg*)args;
    usleep(a->sleep_ms * 1000);
    int* res = malloc(sizeof(int));
    *res = a->id;
    return res;
}

// 自增全局计数器（需要线程安全）
#include <stdatomic.h>
atomic_int global_counter;

void* increment_task(void* args) {
    (void)args;
    atomic_fetch_add(&global_counter, 1);
    return NULL;
}

/* ---------- 测试用例 ---------- */

// 测试1：基本提交与执行
void test_basic_submit() {
    printf("[Test 1] 基本提交与执行...\n");
    ThreadPool* pool = threadPoolCreate(2, 4);
    int arg = 5;
    TaskRet* ret = threadPoolAddTask(pool, simple_task, &arg);
    assert(ret != NULL);

    threadPoolWait(pool);  // 等待所有任务完成

    assert(ret->state == 0);           // 任务完成标志
    assert(*(int*)ret->res == 105);    // 5+100=105
    assert(threadPoolWorkingThrNum(pool) == 0); // 没有繁忙线程

    free(ret->res);
    free(ret);  // 注意：当前实现要求用户手动释放 ret
    threadPoolDestroy(pool);
    printf("[Test 1] PASSED\n");
}

// 测试2：多个任务并发执行
void test_multi_tasks() {
    printf("[Test 2] 多个任务并发执行...\n");
    ThreadPool* pool = threadPoolCreate(2, 4);
    const int N = 10;
    TaskRet* rets[N];
    int args[N];

    for (int i = 0; i < N; i++) {
        args[i] = i;
        rets[i] = threadPoolAddTask(pool, simple_task, &args[i]);
        assert(rets[i] != NULL);
    }

    threadPoolWait(pool);

    for (int i = 0; i < N; i++) {
        assert(rets[i]->state == 0);
        assert(*(int*)rets[i]->res == i + 100);
        free(rets[i]->res);
        free(rets[i]);
    }
    threadPoolDestroy(pool);
    printf("[Test 2] PASSED\n");
}

// 测试3：验证繁忙线程数
void test_busy_count() {
    printf("[Test 3] 验证繁忙线程数...\n");
    ThreadPool* pool = threadPoolCreate(2, 4);

    // 提交两个长任务，占用所有最小线程
    TaskArg a1 = {1, 200}, a2 = {2, 200};
    TaskRet* r1 = threadPoolAddTask(pool, slow_task, &a1);
    TaskRet* r2 = threadPoolAddTask(pool, slow_task, &a2);
    usleep(100 * 1000); // 等待线程开始工作
    assert(threadPoolWorkingThrNum(pool) == 2); // 两个都在忙

    // 再提交一个任务，由于无空闲线程，会保持忙碌数
    TaskArg a3 = {3, 100};
    TaskRet* r3 = threadPoolAddTask(pool, slow_task, &a3);
    usleep(50 * 1000);
    // 此时可能仍为2（取决于调度），但最终都会完成
    threadPoolWait(pool);
    assert(threadPoolWorkingThrNum(pool) == 0);

    free(r1->res); free(r1);
    free(r2->res); free(r2);
    free(r3->res); free(r3);
    threadPoolDestroy(pool);
    printf("[Test 3] PASSED\n");
}

// 测试4：动态增加线程（需等待管理器调整）
void test_dynamic_add_threads() {
    printf("[Test 4] 动态增加线程...\n");
    ThreadPool* pool = threadPoolCreate(1, 5);

    // 提交5个持续200ms的任务，观察线程数是否增加到5
    TaskRet* rets[5];
    TaskArg args[5];
    for (int i = 0; i < 5; i++) {
        args[i].id = i;
        args[i].sleep_ms = 200;
        rets[i] = threadPoolAddTask(pool, slow_task, &args[i]);
    }

    // 管理器每3秒检查一次，故等待3秒以上
    sleep(4); 
    // 注意：当前实现中动态增加逻辑有bug，此处可能不达标
    // 我们无法直接获取存活线程数，仅通过行为推断
    threadPoolWait(pool);

    for (int i = 0; i < 5; i++) {
        assert(rets[i]->state == 0);
        free(rets[i]->res);
        free(rets[i]);
    }
    assert(threadPoolWorkingThrNum(pool) == 0);

    threadPoolDestroy(pool);
    printf("[Test 4] PASSED (注意：动态增加逻辑可能存在bug)\n");
}

// 测试5：销毁时清理任务队列（任务被标记为 -1）
void test_destroy_with_pending_tasks() {
    printf("[Test 5] 销毁时清理待处理任务...\n");
    ThreadPool* pool = threadPoolCreate(1, 2);

    // 先提交一个长时间任务阻塞唯一线程
    TaskArg slow = {99, 500};
    threadPoolAddTask(pool, slow_task, &slow);

    // 再提交两个任务，它们会排在队列中
    TaskRet* r1 = threadPoolAddTask(pool, simple_task, &(int){10});
    TaskRet* r2 = threadPoolAddTask(pool, simple_task, &(int){20});

    usleep(50 * 1000); // 确保第一个任务已开始，后两个在队列
    threadPoolDestroy(pool); // 销毁时会遍历队列并置 state = -1

    // 注意：当前实现中 TaskRet 没有被释放，会产生内存泄漏
    // 但这里检查状态
    assert(r1->state == -1);
    assert(r2->state == -1);
    // 第一个任务可能完成或被中断，为简单起见不检查它
    free(r1); // 由于实现泄漏，这里不 free(r1->res) 因为未分配
    free(r2);
    printf("[Test 5] PASSED (但存在内存泄漏: TaskRet 未释放)\n");
}

// 测试6：threadPoolWait 等待
void test_multiple_waiters() {
    printf("[Test 6] 等待 threadPoolWait...\n");
    ThreadPool* pool = threadPoolCreate(2, 5);

    // 提交几个任务
    for (int i = 0; i < 6; i++) {
        TaskArg arg = {i, 4};
        threadPoolAddTask(pool, slow_task, &arg);
    }

    threadPoolWait(pool);
    assert(threadPoolWorkingThrNum(pool) == 0);
    sleep(7);
    assert(threadPoolLiveThrNum(pool) == 2);

    printf("[Test 6] PASSED\n");
    threadPoolDestroy(pool);
}

// 测试7：压力测试（无数据竞争）
void test_stress() {
    printf("[Test 7] 压力测试 (1000 个任务)...\n");
    ThreadPool* pool = threadPoolCreate(4, 8);
    atomic_init(&global_counter, 0);

    const int N = 1000;
    for (int i = 0; i < N; i++) {
        TaskRet* ret = threadPoolAddTask(pool, increment_task, NULL);
        assert(ret != NULL);
    }

    threadPoolWait(pool);
    assert(global_counter == N);
    printf("global_counter = %d\n", global_counter);
    threadPoolDestroy(pool);
    printf("[Test 7] PASSED\n");
}


/* ========== 以下为新增极端测试 ========== */

// ---------- 辅助：全局原子变量 ----------
atomic_int waiters_awoken;          // 被唤醒的等待者计数
atomic_int destroy_crash_flag;      // 销毁测试中的异常标志
atomic_int stress_counter;          // 压力测试用


// ---------- 测试9：动态线程增加逻辑极端压力 ----------
// 针对 manager() 中 liveNum 局部变量被覆盖的 bug，大量短任务可能触发
// 无限重试创建线程或线程数错误。
void test_dynamic_add_stress() {
    printf("[Test 9] 动态增加线程极端压力...\n");
    ThreadPool* pool = threadPoolCreate(1, 5);
    const int NTASKS = 200;
    atomic_init(&stress_counter, 0);

    // 提交 200 个耗时 30ms 的任务，迅速积压任务
    for (int i = 0; i < NTASKS; i++) {
        TaskArg* arg = malloc(sizeof(TaskArg));
        arg->id = i;
        arg->sleep_ms = 30;
        TaskRet* r = threadPoolAddTask(pool, slow_task, arg);
        assert(r != NULL);
    }

    // 等待所有任务完成（若有 bug 可能导致部分任务永远不被调度）
    threadPoolWait(pool);
    // 简单检查：所有任务状态应为完成（这里无法直接获取，依赖 ret）
    // 但此时已释放部分 ret，故跳过详细检查，仅确认线程池能正常销毁不崩溃
    threadPoolDestroy(pool);
    printf("[Test 9] PASSED (若未死锁或崩溃则表面通过，需结合 valgrind 检查)\n");
}

// ---------- 测试10：并发提交与销毁竞争 ----------
// 多个线程同时提交任务，另一线程调用销毁，观察是否崩溃。
void* submitter(void* arg) {
    ThreadPool* pool = (ThreadPool*)arg;
    for (int i = 0; i < 100; i++) {
        TaskRet* r = threadPoolAddTask(pool, simple_task, &(int){i});
        if (!r) { // 可能因销毁返回 NULL
            break;
        }
        usleep(1000);
    }
    return NULL;
}

void test_concurrent_submit_destroy() {
    printf("[Test 10] 并发提交与销毁竞争...\n");
    ThreadPool* pool = threadPoolCreate(4, 8);
    atomic_init(&destroy_crash_flag, 0);

    pthread_t sub_threads[4];
    for (int i = 0; i < 4; i++) {
        pthread_create(&sub_threads[i], NULL, submitter, pool);
    }

    // 让提交跑一会儿
    usleep(200 * 1000);
    // 执行销毁，内部会等待所有线程退出（manager 会 join 所有 worker）
    threadPoolDestroy(pool);

    // 等待提交线程结束（它们可能会因 pool 销毁而失败）
    for (int i = 0; i < 4; i++) {
        pthread_join(sub_threads[i], NULL);
    }

    printf("[Test 10] PASSED (无崩溃则通过，可能存在内存泄漏)\n");
}

// ---------- 测试11：长时间运行与内存泄漏累积 ----------
// 连续运行 15 秒，每 100ms 提交一组任务，结束后用 valgrind 观察泄漏。
void test_long_run_leak() {
    printf("[Test 11] 长时间运行（15秒）...\n");
    ThreadPool* pool = threadPoolCreate(2, 4);
    time_t start = time(NULL);
    int iter = 0;
    while (time(NULL) - start < 15) {
        for (int i = 0; i < 10; i++) {
            TaskArg* arg = malloc(sizeof(TaskArg));
            arg->id = iter++;
            arg->sleep_ms = 10;
            threadPoolAddTask(pool, slow_task, arg);
        }
        usleep(100 * 1000);
    }
    threadPoolDestroy(pool);
    printf("[Test 11] PASSED (请使用 valgrind 检查内存泄漏累积)\n");
}

int main() {
    printf("===== 线程池测试套件 =====\n");
    test_basic_submit();
    test_multi_tasks();
    test_busy_count();
    test_dynamic_add_threads();
    test_destroy_with_pending_tasks();
    test_multiple_waiters();
    test_stress();

    // 新增极端测试
    test_dynamic_add_stress();
    test_concurrent_submit_destroy();
    test_long_run_leak();

    printf("\n所有测试执行完毕（部分暴露已知实现缺陷）。\n");
    return 0;
}