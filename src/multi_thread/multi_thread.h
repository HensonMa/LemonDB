#ifndef LEMONDB_MULTI_THREAD_H
#define LEMONDB_MULTI_THREAD_H

#include <thread>
#include <vector>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <functional>
#include <future>


class Thread_group {
    typedef std::function<void()> task_type;
private:
    std::mutex lock_t;
    std::condition_variable cv;
    std::atomic<bool> finish;
    std::atomic<unsigned int> idle_threadCnt;
    std::vector<std::unique_ptr<std::thread>> thread_list;
    std::queue<task_type> task_queue;

    void thread_begin_work(){
        while (!finish.load()) {
            task_type task;
            {
                std::unique_lock<std::mutex> lock(lock_t);
                while (task_queue.empty()) {
                    cv.wait(lock);
                    if(finish) break;
                }
                if (finish) return;
                task = std::move(task_queue.front());
                task_queue.pop();
            }
            idle_threadCnt--;
            if (task) task();
            idle_threadCnt++;
        }
    }

public:
    explicit Thread_group(){
        finish.store(false);
    }

    void set_thread(unsigned int threadCnt){
        for(unsigned int i=0;i<threadCnt;i++){
            std::unique_ptr<std::thread> tmp (new std::thread(&Thread_group::thread_begin_work, this));
            thread_list.push_back(std::move(tmp));
        }
        idle_threadCnt.store(threadCnt);
    }

    unsigned int count_idle_thread(){
        return idle_threadCnt;
    }

    template<class F_type, class... Args_type>
    auto worker(F_type &&func_t, Args_type &&...args_t) -> std::future<decltype(func_t(args_t...))> {
        std::shared_ptr<std::packaged_task<decltype(func_t(args_t...))()>> task =
                std::make_shared<std::packaged_task<decltype(func_t(args_t...))()>>(
                        std::bind(std::forward<F_type>(func_t), std::forward<Args_type>(args_t)...));
        std::future<decltype(func_t(args_t...))> future = task->get_future();
        std::lock_guard<std::mutex> lock{lock_t};
        task_queue.push([task]() { (*task)(); });
        cv.notify_one();

        return future;
    }

    void close_all_threads(){
        if (!finish.load()) finish.store(true);
        while (!task_queue.empty()) task_queue.pop();
        cv.notify_all();
        for (std::unique_ptr<std::thread> &thread: thread_list){
            if (thread->joinable()) thread->join();
        }
        thread_list.clear();
    }

    ~Thread_group(){
        close_all_threads();
    };
};

extern Thread_group thread_pool;

#endif //LEMONDB_MULTI_THREAD_H
