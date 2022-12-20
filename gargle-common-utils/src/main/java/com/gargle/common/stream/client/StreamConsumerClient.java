package com.gargle.common.stream.client;

import com.gargle.common.stream.context.StreamContext;
import com.gargle.common.stream.link.AbstractStreamLinkExecute;
import com.gargle.common.stream.result.StreamResult;
import com.gargle.common.utils.string.StringUtil;

/**
 * ClassName:StreamConsumerClient
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 14:37
 */
public interface StreamConsumerClient<Recode> {

    default StreamResult process(Recode recode) {
        AbstractStreamLinkExecute streamLinkExecute = getStreamLinkExecute();
        if (streamLinkExecute == null) {
            return StreamResult.fail("streamLinkExecute is null");
        }
        return streamLinkExecute.apply(new StreamContext<>(recode));
    }

    default StreamResult process(Recode recode, String locomotiveNodeName) {
        if (StringUtil.isBlank(locomotiveNodeName)) {
            return StreamResult.fail("locomotiveNodeName is null");
        }
        AbstractStreamLinkExecute streamLinkExecute = getStreamLinkExecute();
        if (streamLinkExecute == null) {
            return StreamResult.fail("streamLinkExecute is null");
        }
        return streamLinkExecute.apply(new StreamContext<>(recode, locomotiveNodeName));
    }

    AbstractStreamLinkExecute getStreamLinkExecute();
}
