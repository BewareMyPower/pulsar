#include "ClientImpl.h"
#include "ReaderImpl.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

ReaderImpl::ReaderImpl(const ClientImplPtr client, const std::string& topic, const ReaderConfiguration& conf,
                       ReaderCallback readerCreatedCallback)
    : topic_(topic), client_(client), readerConf_(conf), readerCreatedCallback_(readerCreatedCallback) {}

void ReaderImpl::start(const MessageId& startMessageId) {
    readerCreatedCallback_(ResultOk, Reader(shared_from_this()));
}

}  // namespace pulsar
