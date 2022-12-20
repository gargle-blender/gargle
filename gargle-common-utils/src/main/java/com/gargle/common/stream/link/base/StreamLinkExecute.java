package com.gargle.common.stream.link.base;

import com.gargle.common.serializable.Serializable;
import com.gargle.common.stream.result.StreamResult;

import java.util.function.Function;

/**
 * ClassName:LinkExecute
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 14:38
 */
public interface StreamLinkExecute<Context> extends Function<Context, StreamResult>, Serializable {

    @Override
    StreamResult apply(Context context);
}
