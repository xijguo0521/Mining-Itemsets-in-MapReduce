import java.util.*;

public class MRApriori {

    private List<Set<Integer>> freqItemsets;

    public MRApriori() {
        freqItemsets = new ArrayList<>();
    }


    public void mineFIs(List<Set<Integer>> dataset, int minSupp) {
       int k = 1;
       Map<Integer, List<Set<Integer>>> FIBySize = new HashMap<>();
       Map<Set<Integer>, Integer> itemsetsSupps = new HashMap<>();
       FIBySize.put(k, findSizeOneFreqItemsets(dataset, itemsetsSupps, minSupp));

       while (!FIBySize.get(k).isEmpty()) {
           List<Set<Integer>> lastSizeFI = FIBySize.get(k);
           List<Set<Integer>> candidates = generateCandidates(k, lastSizeFI);
           List<Set<Integer>> FIs = findFIBySize(candidates, dataset, minSupp);
           FIBySize.put(++k, FIs);
       }

       for (Map.Entry<Integer, List<Set<Integer>>> entry : FIBySize.entrySet()) {
           freqItemsets.addAll(entry.getValue());
       }
    }

    private List<Set<Integer>> findFIBySize(List<Set<Integer>> candidates,
                                                                   List<Set<Integer>> dataset, int minSupp) {
        Map<Set<Integer>, Integer> candidatesSupp = new HashMap<>();

        for (Set<Integer> t : dataset) {
            for (Set<Integer> c : candidates) {
                if (t.containsAll(c)) {
                    candidatesSupp.put(c, candidatesSupp.getOrDefault(c, 0) + 1);
                }
            }
        }

        List<Set<Integer>> FIs = new ArrayList<>();

        for (Map.Entry<Set<Integer>, Integer> entry : candidatesSupp.entrySet()) {
            if (entry.getValue() >= minSupp)
                FIs.add(entry.getKey());
        }

        return FIs;
    }

    private List<Set<Integer>> generateCandidates(int k, List<Set<Integer>> lastSizeFI) {
        List<Set<Integer>> candidates = new ArrayList<>();

        // Generate all candidates of size 2
        if (k == 1) {
            for (int i = 0; i < lastSizeFI.size(); i++) {
                for (int j = i + 1; j < lastSizeFI.size() ; j++) {
                    Set<Integer> c = new HashSet<>(lastSizeFI.get(i));
                    c.addAll(lastSizeFI.get(j));
                    candidates.add(c);
                }
            }
            return candidates;
        }

        List<List<Integer>> lastSizeFIList = new ArrayList<>();
        for (Set<Integer> FI : lastSizeFI) {
            List<Integer> FL = new ArrayList<>(FI);
            Collections.sort(FL, new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1 - o2;
                }
            });
            lastSizeFIList.add(FL);
        }

        for (int i = 0; i < lastSizeFIList.size(); i++) {
            inner: for (int j = i + 1; j < lastSizeFIList.size(); j++) {
                List<Integer> listA = lastSizeFIList.get(i);
                List<Integer> listB = lastSizeFIList.get(j);

                for (int r = 0; r < listA.size() - 1; r++) {
                    if (listA.get(r) != listB.get(r))
                        continue inner;
                }

                // The first K-1 elements in listA and listB are the same so that we can generate a new candidate
                Set<Integer> newCandidate = new HashSet<>();
                newCandidate.addAll(listA);
                newCandidate.add(listB.get(listB.size() - 1));

                candidates.add(newCandidate);
            }
        }

        return candidates;
    }

    private List<Set<Integer>> findSizeOneFreqItemsets(List<Set<Integer>> dataset, Map<Set<Integer>,
            Integer> itemsetsSupps, int minSupp) {
        List<Set<Integer>> sizeOneFIs = new ArrayList<>();
        Map<Integer, Integer> sizeOneItemsetSupp = new HashMap<>();

        for (Set<Integer> itemset : dataset) {
            for (Integer item : itemset) {
                Set<Integer> s = new HashSet<>();
                s.add(item);

                if (itemsetsSupps.containsKey(s))
                    itemsetsSupps.replace(s, itemsetsSupps.get(s) + 1);
                else
                    itemsetsSupps.put(s, 1);

                sizeOneItemsetSupp.put(item, sizeOneItemsetSupp.getOrDefault(item, 0) + 1);
            }
        }

        List<Integer> temp = new ArrayList<>();

        for (Map.Entry<Integer, Integer> entry : sizeOneItemsetSupp.entrySet()) {
            if (entry.getValue() >= minSupp)
                temp.add(entry.getKey());
        }


        // Make sure the frequent itemsets of size 1 is in lexical order
        Collections.sort(temp);
        for (Integer i : temp) {
            Set<Integer> sizeOneFI = new HashSet<>();
            sizeOneFI.add(i);
            sizeOneFIs.add(sizeOneFI);
        }

        return sizeOneFIs;
    }

    public List<Set<Integer>> getFreqItemsets() {
        return freqItemsets;
    }
}
