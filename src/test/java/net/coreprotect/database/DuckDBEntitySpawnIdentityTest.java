package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.UUID;

import org.bukkit.Location;
import org.bukkit.World;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.statement.EntitySpawnStatement;
import net.coreprotect.model.entity.EntityInteractionOrigin;
import net.coreprotect.model.entity.EntitySpawnIdentity;

class DuckDBEntitySpawnIdentityTest {

    private static final String PREFIX = "identity_test_";
    private static final String WORLD_NAME = "coreprotect_identity_regression";
    private static final EntityInteractionOrigin ORIGIN = new EntityInteractionOrigin(3, -12.5, 64.25, 18.75);

    private DatabaseType previousDatabaseType;
    private String previousPrefix;
    private Integer previousWorldId;
    private Connection connection;
    private RelationalConsumerWriteBatch batch;
    private Location currentLocation;

    @BeforeEach
    void setUp() throws Exception {
        previousDatabaseType = ConfigHandler.databaseType;
        previousPrefix = ConfigHandler.prefix;
        previousWorldId = ConfigHandler.worlds.put(WORLD_NAME, 7);
        ConfigHandler.databaseType = DatabaseType.DUCKDB;
        ConfigHandler.prefix = PREFIX;

        World world = mock(World.class);
        when(world.getName()).thenReturn(WORLD_NAME);
        currentLocation = new Location(world, 100.5, 70.25, -200.75, 45.0F, 10.0F);
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SEQUENCE " + PREFIX + "entity_spawn_rowid_seq START 1");
            statement.execute("CREATE TABLE " + PREFIX + "entity_spawn ("
                    + "rowid INTEGER PRIMARY KEY DEFAULT nextval('" + PREFIX + "entity_spawn_rowid_seq'),"
                    + "time INTEGER,block_rowid BIGINT,kill_rowid INTEGER,uuid VARCHAR UNIQUE NOT NULL,"
                    + "wid INTEGER,current_wid INTEGER,origin_x DOUBLE,origin_y DOUBLE,origin_z DOUBLE,"
                    + "x DOUBLE,y DOUBLE,z DOUBLE,yaw FLOAT,pitch FLOAT,data BLOB,removed INTEGER)");
            statement.execute("CREATE TABLE " + PREFIX + "entity_interaction ("
                    + "time INTEGER," + DatabaseType.DUCKDB.getUserColumn() + " INTEGER,"
                    + "entity_spawn_rowid INTEGER,wid INTEGER,x INTEGER,y INTEGER,z INTEGER,"
                    + "type INTEGER,action INTEGER,metadata BLOB,rolled_back INTEGER)");
        }
        batch = new RelationalConsumerWriteBatch(connection, DatabaseType.DUCKDB);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (connection != null && !connection.isClosed() && !connection.getAutoCommit()) {
                batch.rollback();
            }
            if (batch != null) {
                batch.close();
            }
        }
        finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            }
            finally {
                ConfigHandler.databaseType = previousDatabaseType;
                ConfigHandler.prefix = previousPrefix;
                if (previousWorldId == null) {
                    ConfigHandler.worlds.remove(WORLD_NAME);
                }
                else {
                    ConfigHandler.worlds.put(WORLD_NAME, previousWorldId);
                }
            }
        }
    }

    @Test
    void duplicateUuidDoesNotAbortFollowingInteractionWrites() throws Exception {
        UUID uuid = UUID.randomUUID();
        beginTransaction();
        EntitySpawnIdentity first = insertIdentity(uuid, ORIGIN);
        batch.addEntityInteraction(10, 1, first.getRowId(), 7, 100, 70, -201, 1, 0, null, 0);

        EntitySpawnIdentity duplicate = insertIdentity(uuid, ORIGIN);
        assertEquals(first.getRowId(), duplicate.getRowId());
        batch.addEntityInteraction(11, 1, duplicate.getRowId(), 7, 100, 70, -201, 1, 1, null, 0);
        EntitySpawnIdentity unrelated = insertIdentity(UUID.randomUUID(), ORIGIN);
        assertTrue(unrelated.getRowId() > first.getRowId());
        assertFalse(Database.isTransactionRollbackOnly());
        assertTrue(batch.commit());
        assertEquals(2, count("entity_spawn"));
        assertEquals(2, count("entity_interaction"));
        assertEquals(2, scalar("SELECT COUNT(*) FROM " + PREFIX + "entity_interaction WHERE entity_spawn_rowid=" + first.getRowId()));
    }

    @Test
    void stalePrefetchMissResolvesThePersistedIdentity() throws Exception {
        UUID uuid = UUID.randomUUID();
        assertTrue(EntitySpawnStatement.loadIdentities(connection, Collections.singleton(uuid)).isEmpty());
        int existingRowId = seedIdentity(uuid, 0, null, null, null);

        beginTransaction();
        EntitySpawnIdentity identity = insertIdentity(uuid, new EntityInteractionOrigin(99, 1, 2, 3));
        assertEquals(existingRowId, identity.getRowId());
        assertOriginalIdentity(identity);
        assertFalse(identity.hasSpawnLog());
        assertTrue(batch.commit());
        assertEquals(1, count("entity_spawn"));
    }

    @Test
    void duplicateDoesNotOverwriteHistoryOrReviveARemovedEntity() throws Exception {
        UUID uuid = UUID.randomUUID();
        byte[] state = { 1, 2, 3, 4 };
        int existingRowId = seedIdentity(uuid, 1, 101L, 202, state);

        beginTransaction();
        EntitySpawnIdentity identity = insertIdentity(uuid, new EntityInteractionOrigin(99, 1, 2, 3));
        assertEquals(existingRowId, identity.getRowId());
        assertOriginalIdentity(identity);
        assertTrue(identity.hasSpawnLog());
        assertFalse(batch.checkpointEntitySpawn(identity.getRowId(), 9, 1, 2, 3, 0, 0));
        assertTrue(batch.commit());
        try (Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("SELECT * FROM " + PREFIX + "entity_spawn")) {
            assertTrue(row.next());
            assertEquals(5, row.getInt("time"));
            assertEquals(101L, row.getLong("block_rowid"));
            assertEquals(202, row.getInt("kill_rowid"));
            assertEquals(1, row.getInt("removed"));
            assertEquals(7, row.getInt("current_wid"));
            assertEquals(100.5, row.getDouble("x"));
            assertArrayEquals(state, row.getBytes("data"));
            assertFalse(row.next());
        }
    }

    @Test
    void freshBatchReusesCommittedIdentity() throws Exception {
        UUID uuid = UUID.randomUUID();
        beginTransaction();
        int rowId = insertIdentity(uuid, ORIGIN).getRowId();
        assertTrue(batch.commit());
        batch.close();
        batch = new RelationalConsumerWriteBatch(connection, DatabaseType.DUCKDB);

        beginTransaction();
        assertEquals(rowId, insertIdentity(uuid, ORIGIN).getRowId());
        assertTrue(batch.commit());
        assertEquals(1, count("entity_spawn"));
    }

    @Test
    void rolledBackIdentityCanBeRetriedWithoutAStaleRowId() throws Exception {
        UUID uuid = UUID.randomUUID();
        beginTransaction();
        insertIdentity(uuid, ORIGIN);
        batch.rollback();
        assertEquals(0, count("entity_spawn"));

        beginTransaction();
        EntitySpawnIdentity retried = insertIdentity(uuid, ORIGIN);
        assertNotNull(retried);
        assertTrue(batch.commit());
        assertEquals(retried.getRowId(), scalar("SELECT rowid FROM " + PREFIX + "entity_spawn"));
    }

    @Test
    void unrelatedPrimaryKeyConflictStillFailsAndRollsBack() throws Exception {
        UUID storedUuid = UUID.randomUUID();
        try (Statement statement = connection.createStatement()) {
            // Leave the sequence at 1 so the next insert collides on rowid, not uuid.
            statement.execute("INSERT INTO " + PREFIX + "entity_spawn (rowid,uuid,removed) VALUES (1,'" + storedUuid + "',0)");
        }
        beginTransaction();
        assertThrows(SQLException.class, () -> insertIdentity(UUID.randomUUID(), ORIGIN));
        assertTrue(Database.isTransactionRollbackOnly());
        assertFalse(batch.commit());
        assertEquals(1, count("entity_spawn"));
        assertEquals(Integer.valueOf(1), EntitySpawnStatement.findRowIdByUuid(connection, storedUuid));
    }

    private void beginTransaction() throws Exception {
        // Use the real transaction/savepoint/commit path without initializing the unrelated spatial index.
        try (Statement statement = connection.createStatement()) {
            Database.beginTransaction(statement, DatabaseType.DUCKDB);
        }
    }

    private EntitySpawnIdentity insertIdentity(UUID uuid, EntityInteractionOrigin origin) throws Exception {
        EntitySpawnIdentity[] identity = new EntitySpawnIdentity[1];
        batch.executeAtomically("entity_interaction_log", () -> {
            identity[0] = EntitySpawnStatement.insertIdentity(batch, 10, uuid, origin, currentLocation);
        });
        return identity[0];
    }

    private int seedIdentity(UUID uuid, int removed, Long blockRowId, Integer killRowId, byte[] state) throws Exception {
        return batch.addEntitySpawn(5, blockRowId, killRowId, uuid, ORIGIN.getWorldId(), 7,
                ORIGIN.getX(), ORIGIN.getY(), ORIGIN.getZ(), 100.5, 70.25, -200.75, 45, 10, state, removed);
    }

    private void assertOriginalIdentity(EntitySpawnIdentity identity) {
        assertEquals(ORIGIN.getWorldId(), identity.getOriginalWorldId());
        assertEquals(-13, identity.getOriginalX());
        assertEquals(64, identity.getOriginalY());
        assertEquals(18, identity.getOriginalZ());
    }

    private int count(String table) throws SQLException {
        return scalar("SELECT COUNT(*) FROM " + PREFIX + table);
    }

    private int scalar(String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        }
    }
}
