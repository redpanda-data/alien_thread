/*
 * LGPL licensed utility adapted from Ceph [0].
 *
 * Modifications
 *  - formatting and code conslidation
 *
 *  - using a future<>/exceptional-future<> protocol between seastar and native
 *  threads. the original version used a generalized future return, but the code
 *  was not working as expected. revisting this and generalizing the interface
 *  would be beneficial.
 *
 * [0]: https://github.com/ceph/ceph @ 4589fff6bff8dadd7347fccfc62ed4a49e2b101d
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>

#include <boost/lockfree/queue.hpp>
#include <boost/optional.hpp>

#include <atomic>
#include <condition_variable>
#include <tuple>
#include <type_traits>

namespace v {

class Condition {
    seastar::file_desc file_desc;
    int fd;
    seastar::pollable_fd_state fd_state;
    eventfd_t event = 0;

public:
    Condition()
      : file_desc{seastar::file_desc::eventfd(0, 0)}
      , fd(file_desc.get())
      , fd_state{std::move(file_desc)} {
    }
    seastar::future<> wait() {
        return seastar::engine()
          .read_some(fd_state, &event, sizeof(event))
          .then([](size_t) { return seastar::now(); });
    }
    void notify() {
        eventfd_t result = 1;
        ::eventfd_write(fd, result);
    }
};

struct WorkItem {
    virtual ~WorkItem() {
    }
    virtual void process() = 0;
};

template<typename Func>
//, typename... T = std::invoke_result_t<Func>>
struct Task final : WorkItem {
    Func func;
    seastar::future_state<> state;
    Condition on_done;

public:
    explicit Task(Func&& f)
      : func(std::move(f)) {
    }
    void process() override {
        try {
            func();
            state.set();
        } catch (...) {
            state.set_exception(std::current_exception());
        }
        on_done.notify();
    }
    seastar::future<> get_future() {
        return on_done.wait().then([this] {
            if (state.failed()) {
                return seastar::make_exception_future<>(
                  std::move(state).get_exception());
            } else {
                return seastar::make_ready_future<>();
            }
        });
    }
};

struct SubmitQueue {
    seastar::semaphore free_slots;
    seastar::gate pending_tasks;
    explicit SubmitQueue(size_t num_free_slots)
      : free_slots(num_free_slots) {
    }
    seastar::future<> stop() {
        return pending_tasks.close();
    }
};

/// an engine for scheduling non-seastar tasks from seastar fibers
class ThreadPool {
    std::atomic<bool> stopping = false;
    std::mutex mutex;
    std::condition_variable cond;
    std::vector<std::thread> threads;
    seastar::sharded<SubmitQueue> submit_queue;
    const size_t queue_size;
    boost::lockfree::queue<WorkItem*> pending;

    void loop() {
        for (;;) {
            WorkItem* work_item = nullptr;
            {
                std::unique_lock lock{mutex};
                cond.wait_for(
                  lock, std::chrono::milliseconds(10), [this, &work_item] {
                      return pending.pop(work_item) || is_stopping();
                  });
            }
            if (work_item) {
                work_item->process();
            } else if (is_stopping()) {
                break;
            }
        }
    }
    bool is_stopping() const {
        return stopping.load(std::memory_order_relaxed);
    }
    static void pin(unsigned cpu_id) {
        cpu_set_t cs;
        CPU_ZERO(&cs);
        CPU_SET(cpu_id, &cs);
        [[maybe_unused]] auto r = pthread_setaffinity_np(
          pthread_self(), sizeof(cs), &cs);
    }
    seastar::semaphore& local_free_slots() {
        return submit_queue.local().free_slots;
    }
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

public:
    /**
     * @param queue_sz the depth of pending queue. before a task is scheduled,
     *                 it waits in this queue. we will round this number to
     *                 multiple of the number of cores.
     * @param n_threads the number of threads in this thread pool.
     * @param cpu the CPU core to which this thread pool is assigned
     * @note each @c Task has its own crimson::thread::Condition, which
     * possesses possesses an fd, so we should keep the size of queue under a
     * reasonable limit.
     */
    ThreadPool(size_t n_threads, size_t queue_size, unsigned cpu_id)
      : queue_size{1} // round_up_to(queue_sz, seastar::smp::count)}
      , pending{queue_size} {
        for (size_t i = 0; i < n_threads; i++) {
            threads.emplace_back([this, cpu_id] {
                pin(cpu_id);
                loop();
            });
        }
    }
    ~ThreadPool() {
        for (auto& thread : threads) {
            thread.join();
        }
    }
    seastar::future<> start() {
        auto slots_per_shard = queue_size / seastar::smp::count;
        return submit_queue.start(slots_per_shard);
    }
    seastar::future<> stop() {
        return submit_queue.stop().then([this] {
            stopping = true;
            cond.notify_all();
        });
    }
    template<typename Func, typename... Args>
    auto submit(Func&& func, Args&&... args) {
        auto packaged = [func = std::move(func),
                         args = std::forward_as_tuple(args...)] {
            return std::apply(std::move(func), std::move(args));
        };
        return seastar::with_gate(
          submit_queue.local().pending_tasks,
          [packaged = std::move(packaged), this] {
              return local_free_slots().wait().then(
                [packaged = std::move(packaged), this] {
                    auto task = new Task{std::move(packaged)};
                    auto fut = task->get_future();
                    pending.push(task);
                    cond.notify_one();
                    return fut.finally([task, this] {
                        local_free_slots().signal();
                        delete task;
                    });
                });
          });
    }
};

} // namespace v
