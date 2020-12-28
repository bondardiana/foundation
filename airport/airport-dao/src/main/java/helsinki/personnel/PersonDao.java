package helsinki.personnel;

import static java.lang.String.format;
import static ua.com.fielden.platform.entity.query.fluent.EntityQueryUtils.fetch;
import static ua.com.fielden.platform.entity.query.fluent.EntityQueryUtils.fetchAll;
import static ua.com.fielden.platform.entity.query.fluent.EntityQueryUtils.from;
import static ua.com.fielden.platform.entity.query.fluent.EntityQueryUtils.select;

import static org.apache.logging.log4j.LogManager.getLogger;
import org.apache.logging.log4j.Logger;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import com.google.inject.Inject;

import helsinki.security.tokens.persistent.Person_CanModify_user_Token;
import helsinki.security.tokens.persistent.Person_CanSave_Token;
import helsinki.security.tokens.persistent.Person_CanDelete_Token;

import ua.com.fielden.platform.dao.CommonEntityDao;
import ua.com.fielden.platform.dao.annotations.SessionRequired;
import ua.com.fielden.platform.entity.annotation.EntityType;
import ua.com.fielden.platform.entity.fetch.IFetchProvider;
import ua.com.fielden.platform.entity.query.IFilter;
import ua.com.fielden.platform.entity.query.fluent.fetch;
import ua.com.fielden.platform.entity.query.model.EntityResultQueryModel;
import ua.com.fielden.platform.error.Result;
import ua.com.fielden.platform.security.Authorise;
import ua.com.fielden.platform.security.user.IUser;
import ua.com.fielden.platform.security.user.User;

/**
 * DAO implementation for companion object {@link PersonCo}.
 *
 * @author Generated
 *
 */
@EntityType(Person.class)
public class PersonDao extends CommonEntityDao<Person> implements PersonCo {

    private static final Logger LOGGER = getLogger(PersonDao.class);

    @Inject
    protected PersonDao(final IFilter filter) {
        super(filter);
    }

    @Override
    public Person new_() {
        return super.new_().setActive(true);
    }

    @Override
    @SessionRequired
    @Authorise(Person_CanSave_Token.class)
    public Person save(final Person person) {
        person.isValid().ifFailure(Result::throwRuntime);
        return super.save(person);
    }

    @Override
    @SessionRequired
    @Authorise(Person_CanDelete_Token.class)
    public int batchDelete(final Collection<Long> entitiesIds) {
        return defaultBatchDelete(entitiesIds);
    }

    @Override
    @SessionRequired
    @Authorise(Person_CanDelete_Token.class)
    public int batchDelete(final List<Person> entities) {
        return defaultBatchDelete(entities);
    }

    @Override
    public Optional<Person> currentPerson(final fetch<Person> fetchPerson) {
        final EntityResultQueryModel<Person> qExecModel = select(Person.class).where().prop("user").eq().val(getUser()).model();
        return getEntityOptional(from(qExecModel).with(fetchPerson).model());
    }

    @Override
    public Optional<Person> currentPerson() {
        return currentPerson(fetch(Person.class));
    }

    @Override
    protected IFetchProvider<Person> createFetchProvider() {
        return PersonCo.FETCH_PROVIDER;
    }

    @Override
    @SessionRequired
    @Authorise(Person_CanModify_user_Token.class)
    public Person makeUser(final Person person) {
        if (person.isAUser()) {
            throw Result.failure(format("Person [%s] is already an application user.", person.getKey()));
        }

        final String email = person.getEmail();
        final boolean active = email != null; // i.e. only make a user active if the email is not null
        final IUser co$User = co$(User.class);
        final User user = co$User.new_().setKey(person.getKey()).setEmail(email).setActive(active);
        user.setDesc(format("User for person [%s].", person.getDesc()));
        final User su = co$User.findByKeyAndFetch(fetchAll(User.class), User.system_users.SU.name());
        if (su != null) {
            user.setBasedOnUser(su);
        } else {
            user.setBase(true);
        }
        
        final User savedUser = co$User.resetPasswd(user, user.getKey()).getKey();
        return save(co$(Person.class).findById(person.getId(), PersonCo.FETCH_PROVIDER.fetchModel())
                    .setUser(savedUser));
    }

}