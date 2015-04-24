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
        public Node(String k, String port) {
            this.key = k;
            this.port = port;
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
        while( !((tmp.compareTo(newNode) < 0 && tmp.next.compareTo(newNode) > 0) ||
                  tmp.compareTo(tmp.next) > 0 && (
                           tmp.compareTo(newNode) < 0 || tmp.next.compareTo(newNode) > 0))) {
            tmp = tmp.next;
        }

        newNode.next = tmp.next;
        newNode.next.prev = newNode;
        tmp.next = newNode;
        newNode.prev = tmp;
    }

    public Boolean belongToSelf(String key) {
        Node newNode = new Node(key, "");
        Node tmp = this.node;
        if (tmp.next == null) {
            return true;
        }
        Log.v("Me Log", this.toString());
        Log.v("Me Log", this.node.key + " prev: " + this.node.prev.key);

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

    public String getSuccessor() {
        if (this.node.next != null)
            return this.node.next.port;
        else
            return null;
    }

    public String getSecondSuccessor() {
        if (this.node.next != null && this.node.next.next != null)
            return this.node.next.next.port;
        else
            return null;
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
