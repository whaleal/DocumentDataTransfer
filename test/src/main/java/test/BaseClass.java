package test;


public abstract class BaseClass<T> {

    public String findClassName(){
        return this.getClass().getSimpleName();
    }


}
