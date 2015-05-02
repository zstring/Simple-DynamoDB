package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

/**
 * Created by zstring on 4/22/15.
 */
public class CircularLinkedList {
    private Node node;

    public static class Node implements Comparable<Node> {
        public Node next;
        private Node prev;
        private String key;
        private String port;
        public boolean active;
        public Node(String k, String port) {
            this.key = k;
            this.port = port;
            this.active = true;
        }

        public int compareTo(Node that) {
            return this.key.compareTo(that.key);
        }
    }

    public String getKey() {
        return this.node.key;
    }

    public String getPort() {
        return this.node.port;
    }

    public CircularLinkedList(String k, String port) {
        this.node = new Node(k, port);
    }

    /**
     * Constructing list with key port id pair
     * msg variable first value is just a flag no use here in this function
     * and then subsequence values are hashed port id and port id respectively
     * and myPort is used to have pointer of this list at myPort
     */
    public CircularLinkedList(String msg, String myPort, Boolean flag) {
        String[] pair = msg.split(" ");
        Node tmpNode = new Node(pair[1], pair[2]);
        Node startNode = tmpNode;
        node = tmpNode;
        Node tmp = null;
        for (int i = 3; i < pair.length; i+=2) {
            tmp = new Node(pair[i], pair[i+1]);
            tmpNode.next = tmp;
            tmp.prev = tmpNode;
            tmpNode = tmpNode.next;
            //Setting pointer of the node to its myport
            if (myPort.equals(pair[i + 1])) {
                node = tmp;
            }
        }
        startNode.prev = tmpNode;
        tmp.next = startNode;
    }

    public void addNode(String k, String port) {
        Node newNode = new Node(k, port);

        Node tmp = this.node;
        if (tmp.next == null) {
            tmp.next = newNode;
            newNode.prev = tmp;

            newNode.next = this.node;
            this.node.prev = newNode;
            return;
        }
        Node tmp_check = this.node;
        while (tmp_check.compareTo(newNode) != 0) {
            tmp_check = tmp_check.next;
            // break if it loops onces
            if (tmp_check.compareTo(this.node) == 0) break;

        }
        if (tmp_check.compareTo(newNode) == 0) {
            tmp_check.active = true;
            return;
        } else {
            while (!((tmp.compareTo(newNode) < 0 && tmp.next.compareTo(newNode) > 0) ||
                    tmp.compareTo(tmp.next) > 0 && (
                            tmp.compareTo(newNode) < 0 || tmp.next.compareTo(newNode) > 0))) {
                tmp = tmp.next;
            }

            newNode.next = tmp.next;
            newNode.next.prev = newNode;
            tmp.next = newNode;
            newNode.prev = tmp;
        }
    }

    public Boolean belongToSelf(String key) {
        Node newNode = new Node(key, "");
        Node tmp = this.node;
        if (tmp.next == null) {
            return true;
        }


        if ((tmp.compareTo(newNode) >= 0 && tmp.prev.compareTo(newNode) < 0) ||
                ((tmp.compareTo(tmp.prev)) < 0 && (
                        tmp.compareTo(newNode) >= 0 || tmp.prev.compareTo(newNode) < 0))) {
            Log.v("Me Log Linked List", "yes Belong to ME");
            return  true;
        } else {
            Log.v("Me Log Linked List", "no doesn't belong to ME " + tmp.port);
            return false;
        }
    }


    public boolean belongToPredecessor(String key) {
        Node newNode = new Node(key, "");
        Node tmp = this.node.prev;
        if (tmp.next == null) {
            return true;
        }

        if ((tmp.compareTo(newNode) >= 0 && tmp.prev.compareTo(newNode) < 0) ||
                ((tmp.compareTo(tmp.prev)) < 0 && (
                        tmp.compareTo(newNode) >= 0 || tmp.prev.compareTo(newNode) < 0))) {
            Log.v("Me Log Linked List", "yes Belong to My Predecessor");
            return  true;
        } else {
            Log.v("Me Log Linked List", "no doesn't belong to My Predecessor " + tmp.port);
            return false;
        }
    }

    public String getSuccessor() {
        if (this.node.next != null)
            return this.node.next.port;
        else {
            Log.wtf("Me Log ", "Why successor is null");
            return null;
        }
    }

    public boolean CheckMySuccessors(String newNodeId) {
        if (this.getSuccessor().equals(newNodeId) || this.getSecondSuccessor().equals(newNodeId)) {
            return true;
        }
        return false;
    }

    public boolean CheckMyPredecessor(String newNodeId) {
        if (this.getPredecessor().equals(newNodeId)) {
            return true;
        }
        return false;
    }

    public String[] getKeySuccessor(String k) {
        String[] results = new String[2];
        String coor_port = getCoordinator(k);
        Node tmp = this.node;
        while (!tmp.port.equals(coor_port)) {
            tmp = tmp.next;
        }
        tmp = tmp.next;
        if (tmp != null) {
            results[0] = tmp.port;
            tmp = tmp.next;
            if (tmp != null) {
                results[1] = tmp.port;
            } else {
                Log.wtf("Me Log ", "Why second corrd  is null");
            }
        }
        else {
            Log.wtf("Me Log ", "Why first corrd is null");
        }
        return results;
    }


    public String getPredecessor() {
        if (this.node.prev != null)
            return this.node.prev.port;
        else {
            Log.wtf("Me Log ", "Why predecessor is null");
            return null;
        }
    }

    public String getSecondSuccessor() {
        if (this.node.next != null && this.node.next.next != null)
            return this.node.next.next.port;
        else {
            Log.wtf("Me Log ", "Why second successor is null");
            return null;
        }
    }

    public String getKeySecondSuccessor(String k) {
        if (this.node.next != null && this.node.next.next != null)
            return this.node.next.next.port;
        else {
            Log.wtf("Me Log ", "Why second successor is null");
            return null;
        }
    }

    public String getCoordinator(String key) {
        Node newNode = new Node(key, "");
        Node tmp = this.node;

        while (!((tmp.compareTo(newNode) >= 0 && tmp.prev.compareTo(newNode) < 0) ||
                ((tmp.compareTo(tmp.prev)) < 0 && (
                        tmp.compareTo(newNode) >= 0 || tmp.prev.compareTo(newNode) < 0)))) {
            tmp = tmp.next;
            Log.v("Me Log", "Tmp.next while getCoordinator");
        }

        if ((tmp.compareTo(newNode) >= 0 && tmp.prev.compareTo(newNode) < 0) ||
                ((tmp.compareTo(tmp.prev)) < 0 && (
                        tmp.compareTo(newNode) >= 0 || tmp.prev.compareTo(newNode) < 0))) {
            Log.v("Me Log", "Its in the range getCoordinator " + tmp.port);
        } else {
            Log.v("Me Log", "Its NOT  the range getCoordinator " + tmp.port);
        }
        return tmp.port;
    }

    /**
     * Getting all the nodes hashed and port in the circular
     * list and send to the node who just joined the system
     * @return
     */
    public String getAllContent() {
        String result = "";
        Node tmp = this.node;
        do {
            result += tmp.key + " " + tmp.port + " ";
            tmp = tmp.next;
        } while (tmp != null && tmp.compareTo(this.node) != 0);

        return result.trim();
    }

    @Override
    public String toString() {
        String result = "";
        Node tmp = this.node;
        do {
            result += tmp.port + " ";
            tmp = tmp.next;
        } while (tmp != null && tmp.compareTo(this.node) != 0);


        return result.trim();
    }
}
