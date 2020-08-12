#include "ClientImpl.h"
#include "ReaderImpl.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

ReaderImpl::ReaderImpl(const std::string& topic, const ReaderConfiguration& conf,
                       ReaderCallback readerCreatedCallback)
    : topic_(topic), readerConf_(conf), readerCreatedCallback_(readerCreatedCallback) {}

void ReaderImpl::start(const MessageId& startMessageId) {
    readerCreatedCallback_(ResultOk, Reader(shared_from_this()));
}

}  // namespace pulsar
