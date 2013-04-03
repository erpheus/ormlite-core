package com.j256.ormlite.dao;

import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.DatabaseTableConfig;

import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.Collection;

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
public class SuperDaoImpl<T,ID> extends BaseDaoImpl<Thread,ID> {

    protected Collection<Dao<T,ID>> subDaos;


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
    public SuperDaoImpl(ConnectionSource connectionSource, Class<Thread> dataClass, Class<?> directSubclasses[]) throws SQLException {
        super(connectionSource, dataClass);
        for (Class<?> clazz: directSubclasses){
            /* The daos for the direct subclasses are stored here for later querying.
             * CreateDao creates a DAO if it doesn't exist, but it's important to
             * store already created ones. */
            subDaos.add((Dao<T,ID>)DaoManager.createDao(this.connectionSource,clazz));
        }
        if (!Modifier.isAbstract(this.dataClass.getModifiers())){
            subDaos.add((Dao<T,ID>)this);
        }
    }
}
