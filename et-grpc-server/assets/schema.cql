create keyspace "et" with replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
create keyspace "data" with replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
create keyspace "stats" with replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

create table if not exists "et"."user"
(
    "id"         int,
    "email"      text,
    "name"       text,
    "sessionKey" text,
    "tag"        text,
    primary key ("id", "email")
);
create table if not exists "et"."campaign"
(
    "id"             int,
    "name"           text,
    "notes"          text,
    "configJson"     text,
    "startTimestamp" bigint,
    "endTimestamp"   bigint,
    "creatorId"      int,
    primary key ("creatorId", "id")
);
create table if not exists "et"."campaignResearchers"
(
    "campaignId"   int,
    "researcherId" int,
    primary key ("campaignId", "researcherId")
);
create table if not exists "et"."dataSource"
(
    "id"        int,
    "name"      text,
    "iconName"  text,
    "creatorId" int,
    primary key ("name", "id")
);
create table if not exists "stats"."campaignParticipantStats"
(
    "joinTimestamp"          bigint,
    "lastHeartbeatTimestamp" bigint,
    "campaignId"             int,
    "userId"                 int,
    primary key ("campaignId", "userId")
);
create table if not exists "stats"."perDataSourceStats"
(
    "amountOfSamples" int,
    "syncTimestamp"   bigint,
    "campaignId"      int,
    "userId"          int,
    "dataSourceId"    int,
    primary key ("campaignId", "userId", "dataSourceId")
);
create table if not exists "et"."directMessage"
(
    "id"           int,
    "timestamp"    bigint,
    "subject"      text,
    "content"      text,
    "read"         boolean,
    "sourceUserId" int,
    "targetUserId" int,
    primary key ("targetUserId", "sourceUserId", "id")
);
create table if not exists "et"."notification"
(
    "id"           int,
    "timestamp"    bigint,
    "subject"      text,
    "content"      text,
    "read"         boolean,
    "campaignId"   int,
    "targetUserId" int,
    primary key ("campaignId", "targetUserId", "id")
);
