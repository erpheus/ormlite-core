package com.j256.ormlite.dao;

import com.j256.ormlite.stmt.*;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseResults;
import com.j256.ormlite.table.DatabaseTable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Class for the Database Access Objects that handle the reading and writing a class from the database
 * and have subclasses also in the database.
 *
 * <p>
 * To specify subclasses you sholud use the directSubclasses @Table annotation field.
 * </p>
 *
 * <p>
 * This class is also {@link Iterable} which means you can do a {@code for (T obj : dao)} type of loop code to iterate
 * through the table of persisted objects. See {@link #iterator()}.
 * </p>
 *
 * <p>
 * <b> NOTE: </b> If you are using the Spring type wiring, {@link #initialize} should be called after all of the set
 * methods. In Spring XML, init-method="initialize" should be used.
 * </p>
 *
 * @param T
 *            The class that the code will be operating on.
 * @param ID
 *            The class of the ID column associated with the class. The T class does not require an ID field. The class
 *            needs an ID parameter however so you can use Void or Object to satisfy the compiler.
 * @author erpheus
 */
public class SuperDaoImpl<T,ID> extends BaseDaoImpl<T,ID> {

    protected Collection<Dao<? extends T,ID>> subDaos = new ArrayList<Dao<? extends T, ID>>();

    /**
     * Checks if a class has subclasses in the database.
     * @param clazz Class to scan
     * @return Whether there are database stored subclasses.
     */
    public static boolean isSuperClass(Class<?> clazz){
        DatabaseTable dt = clazz.getAnnotation(DatabaseTable.class);
        if (dt == null || dt.directSubclasses().length == 0){
            Class<?> c = clazz.getSuperclass();
            if (c == null){
                return false;
            }
            return isSuperClass(c);
        }
        return true;
    }

    public static Method getDeclaredMethod(String name, Class<?>[] argTypes, boolean isSuper){
        try{
            if (isSuper){
                return SuperDaoImpl.class.getMethod(name, argTypes);
            }else{
                return Dao.class.getMethod(name, argTypes);
            }
        }catch (NoSuchMethodException e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Construct our base DAO class. The dataClass provided must have its fields marked with {@link com.j256.ormlite.field.DatabaseField} or
     * javax.persistance annotations.
     *
     * @param connectionSource
     *            Source of our database connections.
     * @param dataClass
     *            Class associated with this Dao. This must match the T class parameter.
     * @param directSubclasses
     *            Subclasses of this class with no database intermediate.
     */
    @SuppressWarnings("unchecked")
    public SuperDaoImpl(ConnectionSource connectionSource, Class<T> dataClass, Class<?> directSubclasses[]) throws SQLException {
        super(connectionSource, dataClass);
        for (Class<?> clazz: directSubclasses){
            /* The daos for the direct subclasses are stored here for later querying.
             * CreateDao creates a DAO if it doesn't exist, but it's important to
             * store already created ones. */
            subDaos.add((Dao<T,ID>)DaoManager.createDao(this.connectionSource,clazz));
        }
        if (!Modifier.isAbstract(this.dataClass.getModifiers())){
            subDaos.add(this);
        }
    }

    public List<T> executeQuery(){
        return null;
    }

    private <R,S> R iterateMethod(Method method, Method superMethod, Object[] arguments, IteratingStepCombinator<R,S> combinator, R initialObject){
        S step;
        R result = initialObject;
        try{
            for (Dao<? extends T, ID> dao: this.subDaos){
                if (dao == this){
                    step = (S)superMethod.invoke(this,arguments);
                }else{
                    step = (S)method.invoke(dao,arguments);
                }
                result = combinator.combine(result,step);
                if (combinator.hasToStop){
                    return result;
                }
            }
        }catch (IllegalAccessException e){
            e.printStackTrace();
        }catch (InvocationTargetException e){
            e.printStackTrace();
        }
        return result;
    }

    private <C> C runMethodForSubdao(Method method, Object[] arguments, Class<?> subClass){
        Object result = null;
        forLoop: for (Dao<? extends T,ID> dao: subDaos){
            if (dao.getDataClass().isAssignableFrom(subClass)){
                try{
                    result = method.invoke(dao,arguments);
                    break forLoop;
                }catch (IllegalAccessException e){
                    e.printStackTrace();
                }catch (InvocationTargetException e){
                    e.printStackTrace();
                }
            }
        }
        return (C)result;
    }


    public T queryForId(ID id,Class<?> searchedClass) throws SQLException {
        if (searchedClass == this.dataClass){
            return super.queryForId(id);
        }else{
            Class<?>[] paramTypes = {Object.class};
            Object[] params = {id};
            return this.runMethodForSubdao(getDeclaredMethod("queryForId", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public T queryForFirst(PreparedQuery<T> preparedQuery) throws SQLException {
        return super.queryForFirst(preparedQuery);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public List<T> queryForAll() throws SQLException {
        List<T> list = new ArrayList<T>();
        IteratingStepListCombinator<T> it = new IteratingStepListCombinator<T>();
        Class<?> argTypes[] = {};
        Object args[] = {};
        return this.iterateMethod(getDeclaredMethod("queryForAll",argTypes, false),getDeclaredMethod("superQueryForAll",argTypes, true),args,it,list);
    }
    public List<T> superQueryForAll() throws SQLException {
        return super.queryForAll();
    }

    @Override
    public List<T> queryForEq(String fieldName, Object value) throws SQLException {
        List<T> list = new ArrayList<T>();
        IteratingStepListCombinator<T> it = new IteratingStepListCombinator<T>();
        Class<?> argTypes[] = {String.class,Object.class};
        Object args[] = {fieldName,value};
        return this.iterateMethod(getDeclaredMethod("queryForEq",argTypes, false),getDeclaredMethod("superQueryForEq",argTypes, true),args,it,list);
    }
    public List<T> superQueryForEq(String fieldName, Object value) throws SQLException {
        return super.queryForEq(fieldName,value);
    }

    @Override
    public QueryBuilder<T, ID> queryBuilder() {
        return super.queryBuilder();
    }

    @Override
    public UpdateBuilder<T, ID> updateBuilder() {
        return super.updateBuilder();
    }

    @Override
    public DeleteBuilder<T, ID> deleteBuilder() {
        return super.deleteBuilder();
    }

    @Override
    public List<T> query(PreparedQuery<T> preparedQuery) throws SQLException {
        return super.query(preparedQuery);
    }

    @Override
    public List<T> queryForMatching(T matchObj) throws SQLException {
        Class<?> searchedClass = matchObj.getClass();
        if (searchedClass == this.dataClass){
            return super.queryForMatching(matchObj);
        }else{
            Class<?>[] paramTypes = {matchObj.getClass()};
            Object[] params = {matchObj};
            return this.runMethodForSubdao(getDeclaredMethod("queryForMatching", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public List<T> queryForMatchingArgs(T matchObj) throws SQLException {
        Class<?> searchedClass = matchObj.getClass();
        if (searchedClass == this.dataClass){
            return super.queryForMatchingArgs(matchObj);
        }else{
            Class<?>[] paramTypes = {matchObj.getClass()};
            Object[] params = {matchObj};
            return this.runMethodForSubdao(getDeclaredMethod("queryForMatchingArgs", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public List<T> queryForFieldValues(Map<String, Object> fieldValues) throws SQLException {
        List<T> list = new ArrayList<T>();
        IteratingStepListCombinator<T> it = new IteratingStepListCombinator<T>();
        Class<?> argTypes[] = {fieldValues.getClass()};
        Object args[] = {fieldValues};
        return this.iterateMethod(getDeclaredMethod("queryForFieldValues",argTypes, false),getDeclaredMethod("superQueryForFieldValues",argTypes, true),args,it,list);
    }
    public List<T> superQueryForFieldValues(Map<String, Object> fieldValues) throws SQLException {
        return super.queryForFieldValues(fieldValues);
    }

    @Override
    public List<T> queryForFieldValuesArgs(Map<String, Object> fieldValues) throws SQLException {
        List<T> list = new ArrayList<T>();
        IteratingStepListCombinator<T> it = new IteratingStepListCombinator<T>();
        Class<?> argTypes[] = {fieldValues.getClass()};
        Object args[] = {fieldValues};
        return this.iterateMethod(getDeclaredMethod("queryForFieldValuesArgs",argTypes, false),getDeclaredMethod("superQueryForFieldValuesArgs",argTypes, true),args,it,list);
    }
    public List<T> superQueryForFieldValuesArgs(Map<String, Object> fieldValues) throws SQLException {
        return super.queryForFieldValuesArgs(fieldValues);
    }

    @Override
    public T queryForSameId(T data) throws SQLException {
        Class<?> searchedClass = data.getClass();
        if (searchedClass == this.dataClass){
            return super.queryForSameId(data);
        }else{
            Class<?>[] paramTypes = {data.getClass()};
            Object[] params = {data};
            return this.runMethodForSubdao(getDeclaredMethod("queryForSameId", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public int create(T data) throws SQLException {
        Class<?> searchedClass = data.getClass();
        if (searchedClass == this.dataClass){
            return super.create(data);
        }else{
            Class<?>[] paramTypes = {data.getClass()};
            Object[] params = {data};
            return this.runMethodForSubdao(getDeclaredMethod("create", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public T createIfNotExists(T data) throws SQLException {
        Class<?> searchedClass = data.getClass();
        if (searchedClass == this.dataClass){
            return super.createIfNotExists(data);
        }else{
            Class<?>[] paramTypes = {data.getClass()};
            Object[] params = {data};
            return this.runMethodForSubdao(getDeclaredMethod("createIfNotExists", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public CreateOrUpdateStatus createOrUpdate(T data) throws SQLException {
        Class<?> searchedClass = data.getClass();
        if (searchedClass == this.dataClass){
            return super.createOrUpdate(data);
        }else{
            Class<?>[] paramTypes = {data.getClass()};
            Object[] params = {data};
            return this.runMethodForSubdao(getDeclaredMethod("createOrUpdate", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public int update(T data) throws SQLException {
        Class<?> searchedClass = data.getClass();
        if (searchedClass == this.dataClass){
            return super.update(data);
        }else{
            Class<?>[] paramTypes = {data.getClass()};
            Object[] params = {data};
            return this.runMethodForSubdao(getDeclaredMethod("update", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public int updateId(T data, ID newId) throws SQLException {
        Class<?> searchedClass = data.getClass();
        if (searchedClass == this.dataClass){
            return super.updateId(data, newId);
        }else{
            Class<?>[] paramTypes = {data.getClass(),newId.getClass()};
            Object[] params = {data,newId};
            return this.runMethodForSubdao(getDeclaredMethod("createOrUpdate", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public int update(PreparedUpdate<T> preparedUpdate) throws SQLException {
        return super.update(preparedUpdate);
    }

    @Override
    public int refresh(T data) throws SQLException {
        Class<?> searchedClass = data.getClass();
        if (searchedClass == this.dataClass){
            return super.refresh(data);
        }else{
            Class<?>[] paramTypes = {data.getClass()};
            Object[] params = {data};
            return this.runMethodForSubdao(getDeclaredMethod("refresh", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public int delete(T data) throws SQLException {
        Class<?> searchedClass = data.getClass();
        if (searchedClass == this.dataClass){
            return super.delete(data);
        }else{
            Class<?>[] paramTypes = {data.getClass()};
            Object[] params = {data};
            return this.runMethodForSubdao(getDeclaredMethod("delete", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public int deleteById(ID id) throws SQLException {
        // TODO: Warning
        return super.deleteById(id);
    }

    public int deleteById(ID id, Class<?>searchedClass) throws SQLException {
        if (searchedClass == this.dataClass){
            return super.deleteById(id);
        }else{
            Class<?>[] paramTypes = {id.getClass()};
            Object[] params = {id};
            return this.runMethodForSubdao(getDeclaredMethod("deleteById", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public int delete(Collection<T> datas) throws SQLException {
        Class<?> searchedClass = null;
        for (T data: datas){
            if (searchedClass != null && searchedClass != data.getClass()){
                throw new SQLException("All deleted objects must be of the same class. Try calling delete on each element.");
            }else if(searchedClass == null){
                searchedClass = data.getClass();
            }
        }
        if (searchedClass == this.dataClass){
            return super.delete(datas);
        }else{
            Class<?>[] paramTypes = {datas.getClass()};
            Object[] params = {datas};
            return this.runMethodForSubdao(getDeclaredMethod("delete", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public int deleteIds(Collection<ID> ids) throws SQLException {
        // TODO: Warning.
        return super.deleteIds(ids);
    }

    @Override
    public int delete(PreparedDelete<T> preparedDelete) throws SQLException {
        // TODO: Warning
        return super.delete(preparedDelete);
    }

    @Override
    public CloseableIterator<T> iterator() {
        // TODO: Warning not yet implemented
        return super.iterator();
    }

    @Override
    public CloseableIterator<T> closeableIterator() {
        // TODO: Warning not yet implemented
        return super.closeableIterator();
    }

    @Override
    public CloseableIterator<T> iterator(int resultFlags) {
        // TODO: Warning not yet imlpemented
        return super.iterator(resultFlags);
    }

    @Override
    public CloseableWrappedIterable<T> getWrappedIterable() {
        // TODO: Warning not yet implemented
        return super.getWrappedIterable();
    }

    @Override
    public CloseableWrappedIterable<T> getWrappedIterable(PreparedQuery<T> preparedQuery) {
        // TODO: Warning
        return super.getWrappedIterable(preparedQuery);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void closeLastIterator() throws SQLException {
        super.closeLastIterator();
    }

    @Override
    public CloseableIterator<T> iterator(PreparedQuery<T> preparedQuery) throws SQLException {
        // TODO: Warning
        return super.iterator(preparedQuery);
    }

    @Override
    public CloseableIterator<T> iterator(PreparedQuery<T> preparedQuery, int resultFlags) throws SQLException {
        // TODO: Warning
        return super.iterator(preparedQuery, resultFlags);
    }

    @Override
    public String objectToString(T data) {
        Class<?> searchedClass = data.getClass();
        if (searchedClass == this.dataClass){
            return super.objectToString(data);
        }else{
            Class<?>[] paramTypes = {data.getClass()};
            Object[] params = {data};
            return this.runMethodForSubdao(getDeclaredMethod("objectToString", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public boolean objectsEqual(T data1, T data2) throws SQLException {
        if (data1.getClass() != data2.getClass()){
            return false;
        }
        Class<?> searchedClass = data1.getClass();
        if (searchedClass == this.dataClass){
            return super.objectsEqual(data1, data2);
        }else{
            Class<?>[] paramTypes = {data1.getClass(),data2.getClass()};
            Object[] params = {data1,data2};
            return this.runMethodForSubdao(getDeclaredMethod("objectsEqual", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public boolean isTableExists() throws SQLException {
        IteratingStepCombinator<Boolean,Boolean> it = new IteratingStepCombinator<Boolean, Boolean>() {
            @Override
            public Boolean combine(Boolean previous, Boolean stepResult) {
                if (!(previous && stepResult)){
                    hasToStop = true;
                    return false;
                }else{
                    return true;
                }
            }
        };
        Class<?> argTypes[] = {};
        Object args[] = {};
        return this.iterateMethod(getDeclaredMethod("isTableExists", argTypes, false), getDeclaredMethod("superIsTableExists", argTypes, true), args, it, true);
    }
    public boolean superIsTableExists() throws SQLException {
        return super.isTableExists();
    }

    @Override
    public long countOf() throws SQLException {
        IteratingStepCombinator<Long,Long> it = new IteratingStepCombinator<Long, Long>() {
            @Override
            public Long combine(Long previous, Long stepResult) {
                return previous + stepResult;
            }
        };
        Class<?> argTypes[] = {};
        Object args[] = {};
        return this.iterateMethod(getDeclaredMethod("countOf", argTypes, false), getDeclaredMethod("superCountOf", argTypes, true), args, it, 0L);
    }
    public long superCountOf() throws SQLException {
        return super.countOf();
    }

    @Override
    public long countOf(PreparedQuery<T> preparedQuery) throws SQLException {
        // TODO: Warning
        return super.countOf(preparedQuery);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void assignEmptyForeignCollection(T parent, String fieldName) throws SQLException {
        Class<?> searchedClass = parent.getClass();
        if (searchedClass == this.dataClass){
            super.assignEmptyForeignCollection(parent, fieldName);
        }else{
            Class<?>[] paramTypes = {parent.getClass(), fieldName.getClass()};
            Object[] params = {parent, fieldName};
            this.runMethodForSubdao(getDeclaredMethod("assignEmptyForeignCollection", paramTypes, false), params, searchedClass);
        }
    }

    @Override
    public <FT> ForeignCollection<FT> getEmptyForeignCollection(String fieldName) throws SQLException {
        //TODO: Maybe catch exception if field corresponds to subclass. Nothing worth doing really.
        return super.getEmptyForeignCollection(fieldName);
    }

    @Override
    public T mapSelectStarRow(DatabaseResults results) throws SQLException {
        return super.mapSelectStarRow(results);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public GenericRowMapper<T> getSelectStarRowMapper() throws SQLException {
        //TODO: Warning
        return super.getSelectStarRowMapper();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public RawRowMapper<T> getRawRowMapper() {
        //TODO: Warning
        return super.getRawRowMapper();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean idExists(ID id) throws SQLException {
        //TODO: Warning
        return super.idExists(id);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public boolean idExists(ID id, Class<?> searchedClass) throws SQLException{
        if (searchedClass == this.dataClass){
            return super.idExists(id);
        }else{
            Class<?>[] paramTypes = {id.getClass()};
            Object[] params = {id};
            return this.runMethodForSubdao(getDeclaredMethod("idExists", paramTypes, false), params, searchedClass);
        }
    }
}
