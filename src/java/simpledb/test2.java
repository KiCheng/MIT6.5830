package simpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class test2 {
    public static <T> Set<Set<T>> enumerateSubsets(List<T> v, int size) {
        // 创建一个包含空集合的集合
        Set<Set<T>> els = new HashSet<>();
        els.add(new HashSet<>());

        // 外层循环，控制子集的大小
        for (int i = 0; i < size; i++) {
            // 创建一个新的集合，用于存储新的子集
            Set<Set<T>> newels = new HashSet<>();
            // 遍历当前所有的子集
            for (Set<T> s : els) {
                // 遍历列表中的每个元素
                for (T t : v) {
                    // 创建一个新的子集，包含当前子集的所有元素
                    Set<T> news = new HashSet<>(s);
                    // 尝试向新的子集中添加元素
                    if (news.add(t))
                        // 如果添加成功，将新的子集添加到新的集合中
                        newels.add(news);
                }
            }
            // 更新当前的子集集合
            els = newels;
        }
        // 返回所有的子集
        return els;
    }

    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            list.add(i);
        }
        Set<Set<Integer>> sets = enumerateSubsets(list, 2);
        for (Set<Integer> set : sets) {
            System.out.println(set);
        }
    }
}
