package com.gargle.common.stream.link.node;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName:Node
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/04/02 10:41
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Node {

    boolean skip = false;

    String nodeName;

    String nextNode;

    public Node(String nodeName, String nextNode) {
        this.nodeName = nodeName;
        this.nextNode = nextNode;
    }
}
