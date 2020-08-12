#ifndef LIB_READERIMPL_H_
#define LIB_READERIMPL_H_

#include "ConsumerImpl.h"

namespace pulsar {

class ReaderImpl;

typedef std::shared_ptr<ReaderImpl> ReaderImplPtr;
typedef std::weak_ptr<ReaderImpl> ReaderImplWeakPtr;

class ReaderImpl : public std::enable_shared_from_this<ReaderImpl> {
   public:
    ReaderImpl(ReaderCallback callback) : callback_(callback) {}

    void start(const MessageId& startMessageId) {
        callback_(ResultOk, Reader(shared_from_this()));
    }

    const std::string& getTopic() const {
        static std::string res = "";
        return res;
    }

    Result readNext(Message& msg) { return ResultOk; }
    Result readNext(Message& msg, int timeoutMs) { return ResultOk; }

    void closeAsync(ResultCallback callback) { callback(ResultOk); }

    void hasMessageAvailableAsync(HasMessageAvailableCallback callback) {}

    void seekAsync(const MessageId& msgId, ResultCallback callback) {}

    void seekAsync(uint64_t timestamp, ResultCallback callback) {}

   private:
    ReaderCallback callback_;
};
}  // namespace pulsar

#endif /* LIB_READERIMPL_H_ */
