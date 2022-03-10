package com.wsk.spark.sentry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.model.db.*;
import org.apache.sentry.provider.cache.SentryPrivilegeCache;
import org.apache.sentry.provider.cache.SimplePrivilegeCache;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.*;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

public class SparkAuthHook {
    private static final Logger LOG = LoggerFactory
            .getLogger(org.apache.sentry.binding.hive.HiveAuthzBindingHook.class);

    public HiveAuthzBinding getHiveAuthzBinding() {
        return hiveAuthzBinding;
    }

    private final HiveAuthzBinding hiveAuthzBinding;
    private final HiveAuthzConf authzConf;
    private Database currDB = Database.ALL;
    private Table currTab;
    private List<AccessURI> udfURIs;
    private AccessURI partitionURI;
    private AccessURI indexURI;
    private Table currOutTab = null;
    private Database currOutDB = null;
    private final static HiveConf hiveConf = new HiveConf();

    // True if this is a basic DESCRIBE <table> operation. False for other DESCRIBE variants
    // like DESCRIBE [FORMATTED|EXTENDED]. Required because Hive treats these stmts as the same
    // HiveOperationType, but we want to enforces different privileges on each statement.
    // Basic DESCRIBE <table> is allowed with only column-level privs, while the variants
    // require table-level privileges.
    public boolean isDescTableBasic = false;

    // Flag that specifies if the operation to validate is an ALTER VIEW AS SELECT.
    // Note: Hive sends CREATEVIEW even if ALTER VIEW AS SELECT is used.

    protected boolean serdeURIPrivilegesEnabled;
    protected final List<String> serdeWhiteList;

    public SparkAuthHook() throws Exception {

        if (hiveConf == null) {
            throw new IllegalStateException("Session HiveConf is null");
        }
        authzConf = loadAuthzConf(hiveConf);
        udfURIs = Lists.newArrayList();
        hiveAuthzBinding = new HiveAuthzBinding(hiveConf, authzConf);

        FunctionRegistry.setupPermissionsForBuiltinUDFs("", HiveAuthzConf.HIVE_UDF_BLACK_LIST);

        String serdeWhiteLists =
                authzConf.get(HiveAuthzConf.HIVE_SENTRY_SERDE_WHITELIST,
                        HiveAuthzConf.HIVE_SENTRY_SERDE_WHITELIST_DEFAULT);
        serdeWhiteList = Arrays.asList(serdeWhiteLists.split(","));
        serdeURIPrivilegesEnabled =
                authzConf.getBoolean(HiveAuthzConf.HIVE_SENTRY_SERDE_URI_PRIVILIEGES_ENABLED,
                        HiveAuthzConf.HIVE_SENTRY_SERDE_URI_PRIVILIEGES_ENABLED_DEFAULT);
    }

    public static HiveAuthzConf loadAuthzConf(HiveConf hiveConf) {
        boolean depreicatedConfigFile = false;
        HiveAuthzConf newAuthzConf = null;
        String hiveAuthzConf = hiveConf.get(HiveAuthzConf.HIVE_SENTRY_CONF_URL);
        if (hiveAuthzConf == null || (hiveAuthzConf = hiveAuthzConf.trim()).length() == 0) {
            hiveAuthzConf = hiveConf.get(HiveAuthzConf.HIVE_ACCESS_CONF_URL);
            depreicatedConfigFile = true;
        }

        if (hiveAuthzConf == null || (hiveAuthzConf = hiveAuthzConf.trim()).length() == 0) {
            throw new IllegalArgumentException("Configuration key " + HiveAuthzConf.HIVE_SENTRY_CONF_URL
                    + " value '" + hiveAuthzConf + "' is invalid.");
        }
        try {
            newAuthzConf = new HiveAuthzConf(new URL(hiveAuthzConf));
        } catch (MalformedURLException e) {
            if (depreicatedConfigFile) {
                throw new IllegalArgumentException("Configuration key " + HiveAuthzConf.HIVE_ACCESS_CONF_URL
                        + " specifies a malformed URL '" + hiveAuthzConf + "'", e);
            } else {
                throw new IllegalArgumentException("Configuration key " + HiveAuthzConf.HIVE_SENTRY_CONF_URL
                        + " specifies a malformed URL '" + hiveAuthzConf + "'", e);
            }
        }
        return newAuthzConf;
    }

    @VisibleForTesting
    protected static AccessURI parseURI(String uri, boolean isLocal)
            throws SemanticException {
        try {
            HiveConf conf = hiveConf;
            String warehouseDir = conf.getVar(ConfVars.METASTOREWAREHOUSE);
            Path warehousePath = new Path(warehouseDir);

            // If warehousePath is an absolute path and a scheme is null and authority is null as well,
            // qualified it with default file system scheme and authority.
            if (warehousePath.isAbsoluteAndSchemeAuthorityNull()) {

                URI defaultUri = FileSystem.getDefaultUri(conf);
                warehousePath = warehousePath.makeQualified(defaultUri, warehousePath);
                warehouseDir = warehousePath.toUri().toString();
            }
            return new AccessURI(PathUtils.parseURI(warehouseDir, uri, isLocal));
        } catch (Exception e) {
            throw new SemanticException("Error parsing URI " + uri + ": " +
                    e.getMessage(), e);
        }
    }

    /**
     * Convert the input/output entities into authorizables. generate
     * authorizables for cases like Database and metadata operations where the
     * compiler doesn't capture entities. invoke the hive binding to validate
     * permissions
     *
     */
    public void auth(HiveOperation stmtOperation, Set<ReadEntity> inputs, Set<WriteEntity> outputs, String user) throws AuthorizationException {
        Set<List<DBModelAuthorizable>> inputHierarchy = new HashSet<List<DBModelAuthorizable>>();
        Set<List<DBModelAuthorizable>> outputHierarchy = new HashSet<List<DBModelAuthorizable>>();
        HiveAuthzPrivileges stmtAuthObject = HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(stmtOperation);

        if (LOG.isDebugEnabled()) {
            LOG.debug("stmtAuthObject.getOperationScope() = " + stmtAuthObject.getOperationScope());
            LOG.debug("context.getInputs() = " + inputs);
            LOG.debug("context.getOutputs() = " + outputs);
        }

        // Workaround to allow DESCRIBE <table> to be executed with only column-level privileges, while
        // still authorizing DESCRIBE [EXTENDED|FORMATTED] as table-level.
        // This is done by treating DESCRIBE <table> the same as SHOW COLUMNS, which only requires column
        // level privs.
        if (isDescTableBasic) {
            stmtAuthObject = HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.SHOWCOLUMNS);
        }

        switch (stmtAuthObject.getOperationScope()) {

            case SERVER:
                // validate server level privileges if applicable. Eg create UDF,register jar etc ..
                List<DBModelAuthorizable> serverHierarchy = new ArrayList<DBModelAuthorizable>();
                serverHierarchy.add(hiveAuthzBinding.getAuthServer());
                inputHierarchy.add(serverHierarchy);
                break;
            case DATABASE:
                // workaround for database scope statements (create/alter/drop db)
                List<DBModelAuthorizable> dbHierarchy = new ArrayList<DBModelAuthorizable>();
                dbHierarchy.add(hiveAuthzBinding.getAuthServer());
                dbHierarchy.add(currDB);
                inputHierarchy.add(dbHierarchy);

                if (currOutDB != null) {
                    List<DBModelAuthorizable> outputDbHierarchy = new ArrayList<DBModelAuthorizable>();
                    outputDbHierarchy.add(hiveAuthzBinding.getAuthServer());
                    outputDbHierarchy.add(currOutDB);
                    outputHierarchy.add(outputDbHierarchy);
                } else {
                    outputHierarchy.add(dbHierarchy);
                }

                getInputHierarchyFromInputs(inputHierarchy, inputs);
                break;
            case TABLE:
                // workaround for add partitions
                if (partitionURI != null) {
                    inputHierarchy.add(ImmutableList.of(hiveAuthzBinding.getAuthServer(), partitionURI));
                }
                if (indexURI != null) {
                    outputHierarchy.add(ImmutableList.of(hiveAuthzBinding.getAuthServer(), indexURI));
                }

                getInputHierarchyFromInputs(inputHierarchy, inputs);
                for (WriteEntity writeEntity : outputs) {
                    if (filterWriteEntity(writeEntity)) {
                        continue;
                    }
                    List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
                    entityHierarchy.add(hiveAuthzBinding.getAuthServer());
                    entityHierarchy.addAll(getAuthzHierarchyFromEntity(writeEntity));
                    outputHierarchy.add(entityHierarchy);
                }
                // workaround for metadata queries.
                // Capture the table name in pre-analyze and include that in the input entity list.
                // the view as output. Having the view as input again will case extra privileges to be given.
                if (currTab != null && stmtOperation != HiveOperation.ALTERVIEW_AS) {
                    List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
                    externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
                    externalAuthorizableHierarchy.add(currDB);
                    externalAuthorizableHierarchy.add(currTab);
                    inputHierarchy.add(externalAuthorizableHierarchy);
                }


                // workaround for DDL statements
                // Capture the table name in pre-analyze and include that in the output entity list
                if (currOutTab != null) {
                    List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
                    externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
                    externalAuthorizableHierarchy.add(currOutDB);
                    externalAuthorizableHierarchy.add(currOutTab);
                    outputHierarchy.add(externalAuthorizableHierarchy);
                }
                break;
            case FUNCTION:
                /* The 'FUNCTION' privilege scope currently used for
                 *  - CREATE TEMP FUNCTION
                 *  - DROP TEMP FUNCTION.
                 */
                if (!udfURIs.isEmpty()) {
                    List<DBModelAuthorizable> udfUriHierarchy = new ArrayList<DBModelAuthorizable>();
                    udfUriHierarchy.add(hiveAuthzBinding.getAuthServer());
                    udfUriHierarchy.addAll(udfURIs);
                    inputHierarchy.add(udfUriHierarchy);
                    for (WriteEntity writeEntity : outputs) {
                        List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
                        entityHierarchy.add(hiveAuthzBinding.getAuthServer());
                        entityHierarchy.addAll(getAuthzHierarchyFromEntity(writeEntity));
                        outputHierarchy.add(entityHierarchy);
                    }
                }
                break;
            case CONNECT:
                /* The 'CONNECT' is an implicit privilege scope currently used for
                 *  - USE <db>
                 *  It's allowed when the user has any privilege on the current database. For application
                 *  backward compatibility, we allow (optional) implicit connect permission on 'default' db.
                 */
                List<DBModelAuthorizable> connectHierarchy = new ArrayList<DBModelAuthorizable>();
                connectHierarchy.add(hiveAuthzBinding.getAuthServer());
                // by default allow connect access to default db
                Table currTbl = Table.ALL;
                Column currCol = Column.ALL;
                if ((DEFAULT_DATABASE_NAME.equalsIgnoreCase(currDB.getName()) &&
                        "false".equalsIgnoreCase(authzConf.
                                get(HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), "false")))) {
                    currDB = Database.ALL;
                    currTbl = Table.SOME;
                }

                connectHierarchy.add(currDB);
                connectHierarchy.add(currTbl);
                connectHierarchy.add(currCol);

                inputHierarchy.add(connectHierarchy);
                outputHierarchy.add(connectHierarchy);
                break;
            case COLUMN:
                for (ReadEntity readEntity : inputs) {
                    if (readEntity.getAccessedColumns() != null && !readEntity.getAccessedColumns().isEmpty()) {
                        addColumnHierarchy(inputHierarchy, readEntity);
                    } else {
                        List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
                        entityHierarchy.add(hiveAuthzBinding.getAuthServer());
                        entityHierarchy.addAll(getAuthzHierarchyFromEntity(readEntity));
                        entityHierarchy.add(Column.ALL);
                        inputHierarchy.add(entityHierarchy);
                    }
                }
                break;
            default:
                throw new AuthorizationException("Unknown operation scope type " +
                        stmtAuthObject.getOperationScope().toString());
        }

        HiveAuthzBinding binding;
        try {
            binding = getHiveBindingWithPrivilegeCache(hiveAuthzBinding, user);
        } catch (SemanticException e) {
            // Will use the original hiveAuthzBinding
            binding = hiveAuthzBinding;
        }
        // validate permission
        binding.authorize(stmtOperation, stmtAuthObject, new Subject(user), inputHierarchy,
                outputHierarchy);
    }

    // Build the hierarchy of authorizable object for the given entity type.
    private List<DBModelAuthorizable> getAuthzHierarchyFromEntity(Entity entity) {
        List<DBModelAuthorizable> objectHierarchy = new ArrayList<DBModelAuthorizable>();
        switch (entity.getType()) {
            case TABLE:
                objectHierarchy.add(new Database(entity.getTable().getDbName()));
                objectHierarchy.add(new Table(entity.getTable().getTableName()));
                break;
            case PARTITION:
            case DUMMYPARTITION:
                objectHierarchy.add(new Database(entity.getPartition().getTable().getDbName()));
                objectHierarchy.add(new Table(entity.getPartition().getTable().getTableName()));
                break;
            case DFS_DIR:
            case LOCAL_DIR:
                try {
                    objectHierarchy.add(parseURI(entity.toString(),
                            entity.getType().equals(Entity.Type.LOCAL_DIR)));
                } catch (Exception e) {
                    throw new AuthorizationException("Failed to get File URI", e);
                }
                break;
            case DATABASE:
            case FUNCTION:
                // TODO use database entities from compiler instead of capturing from AST
                break;
            default:
                throw new UnsupportedOperationException("Unsupported entity type " +
                        entity.getType().name());
        }
        return objectHierarchy;
    }

    /**
     * Add column level hierarchy to inputHierarchy
     *
     * @param inputHierarchy
     * @param entity
     */
    private void addColumnHierarchy(Set<List<DBModelAuthorizable>> inputHierarchy,
                                    ReadEntity entity) {
        List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
        entityHierarchy.add(hiveAuthzBinding.getAuthServer());
        entityHierarchy.addAll(getAuthzHierarchyFromEntity(entity));

        switch (entity.getType()) {
            case TABLE:
            case PARTITION:
                List<String> cols = entity.getAccessedColumns();
                for (String col : cols) {
                    List<DBModelAuthorizable> colHierarchy = new ArrayList<DBModelAuthorizable>(entityHierarchy);
                    colHierarchy.add(new Column(col));
                    inputHierarchy.add(colHierarchy);
                }
                break;
            default:
                inputHierarchy.add(entityHierarchy);
        }
    }

    /**
     * Get Authorizable from inputs and put into inputHierarchy
     *
     * @param inputHierarchy
     * @param inputs
     */
    public void getInputHierarchyFromInputs(Set<List<DBModelAuthorizable>> inputHierarchy,
                                            Set<ReadEntity> inputs) {
        for (ReadEntity readEntity : inputs) {
            // skip the tables/view that are part of expanded view definition
            // skip the Hive generated dummy entities created for queries like 'select <expr>'
            if (isChildTabForView(readEntity) || isDummyEntity(readEntity)) {
                continue;
            }
            if (readEntity.getAccessedColumns() != null && !readEntity.getAccessedColumns().isEmpty()) {
                addColumnHierarchy(inputHierarchy, readEntity);
            } else {
                List<DBModelAuthorizable> entityHierarchy = new ArrayList<DBModelAuthorizable>();
                entityHierarchy.add(hiveAuthzBinding.getAuthServer());
                entityHierarchy.addAll(getAuthzHierarchyFromEntity(readEntity));
                inputHierarchy.add(entityHierarchy);
            }
        }
    }

    // Check if this write entity needs to skipped
    private boolean filterWriteEntity(WriteEntity writeEntity)
            throws AuthorizationException {
        // skip URI validation for session scratch file URIs
        if (writeEntity.isTempURI()) {
            return true;
        }
        try {
            if (writeEntity.getTyp().equals(Type.DFS_DIR)
                    || writeEntity.getTyp().equals(Type.LOCAL_DIR)) {
                HiveConf conf = hiveConf;
                String warehouseDir = conf.getVar(ConfVars.METASTOREWAREHOUSE);
                URI scratchURI = new URI(PathUtils.parseDFSURI(warehouseDir,
                        conf.getVar(HiveConf.ConfVars.SCRATCHDIR)));
                URI requestURI = new URI(PathUtils.parseDFSURI(warehouseDir,
                        writeEntity.getLocation().getPath()));
                LOG.debug("scratchURI = " + scratchURI + ", requestURI = " + requestURI);
                if (PathUtils.impliesURI(scratchURI, requestURI)) {
                    return true;
                }
                URI localScratchURI = new URI(PathUtils.parseLocalURI(conf.getVar(HiveConf.ConfVars.LOCALSCRATCHDIR)));
                URI localRequestURI = new URI(PathUtils.parseLocalURI(writeEntity.getLocation().getPath()));
                LOG.debug("localScratchURI = " + localScratchURI + ", localRequestURI = " + localRequestURI);
                if (PathUtils.impliesURI(localScratchURI, localRequestURI)) {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new AuthorizationException("Failed to extract uri details", e);
        }
        return false;
    }

    /**
     * Check if the given read entity is a table that has parents of type Table
     * Hive compiler performs a query rewrite by replacing view with its definition. In the process, tt captures both
     * the original view and the tables/view that it selects from .
     * The access authorization is only interested in the top level views and not the underlying tables.
     *
     * @param readEntity
     * @return
     */
    private boolean isChildTabForView(ReadEntity readEntity) {
        // If this is a table added for view, then we need to skip that
        if (!readEntity.getType().equals(Type.TABLE) && !readEntity.getType().equals(Type.PARTITION)) {
            return false;
        }
        if ((readEntity.getParents() != null) && (readEntity.getParents().size() > 0)) {
            for (ReadEntity parentEntity : readEntity.getParents()) {
                if (!parentEntity.getType().equals(Type.TABLE)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    // Check if the given entity is identified as dummy by Hive compilers.
    private boolean isDummyEntity(Entity entity) {
        return entity.isDummy();
    }

    // create hiveBinding with SentryPrivilegeCache
    private static HiveAuthzBinding getHiveBindingWithPrivilegeCache(HiveAuthzBinding hiveAuthzBinding,
                                                                     String userName) throws SemanticException {
        // get the original HiveAuthzBinding, and get the user's privileges by AuthorizationProvider
        AuthorizationProvider authProvider = hiveAuthzBinding.getCurrentAuthProvider();
        Set<String> userPrivileges = authProvider.getPolicyEngine().getPrivileges(
                authProvider.getGroupMapping().getGroups(userName), Sets.newHashSet(userName),
                hiveAuthzBinding.getActiveRoleSet(), hiveAuthzBinding.getAuthServer());

        // create SentryPrivilegeCache using user's privileges
        SentryPrivilegeCache privilegeCache = new SimplePrivilegeCache(userPrivileges);
        try {
            // create new instance of HiveAuthzBinding whose backend provider should be SimpleSentryCacheProviderBackend
            return new HiveAuthzBinding(HiveAuthzBinding.HiveHook.HiveServer2, hiveAuthzBinding.getHiveConf(),
                    hiveAuthzBinding.getAuthzConf(), privilegeCache);
        } catch (Exception e) {
            LOG.error("Can not create HiveAuthzBinding with privilege cache.");
            throw new SemanticException(e);
        }
    }
}
