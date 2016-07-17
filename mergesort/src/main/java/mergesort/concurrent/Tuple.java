package mergesort.concurrent;

public class Tuple<T1 extends Comparable<? super T1>, T2>
        implements Comparable<Tuple<T1, T2>> {
    private T1 type1;
    private T2 type2;

    public Tuple(T1 t1, T2 t2) {
        this.type1 = t1;
        this.type2 = t2;
    }

    public T2 getT2() {
        return type2;
    }

    public T1 getT1() {
        return type1;
    }

    @Override
    public int compareTo(Tuple<T1, T2> tuple) {
        return type1.compareTo(tuple.getT1());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((type1 == null) ? 0 : type1.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("unchecked")
        Tuple<T1, T2> other = (Tuple<T1, T2>) obj;

        if (type1 == null) {
            if (other.type1 != null)
                return false;
        } else if (!type1.equals(other.type1))
            return false;
        return true;
    }

    public synchronized void setT1(T1 string) {
        this.type1 = string;
    }

    public synchronized void setT2(T2 br) {
        this.type2 = br;
    }

    @Override
    public String toString() {
        return "Tuple [type1=" + type1 + ", type2=" + type2 + "]";
    }
}
