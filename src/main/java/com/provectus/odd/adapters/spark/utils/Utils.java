package com.provectus.odd.adapters.spark.utils;

public class Utils {

    private static final String SLASH_DELIMITER_USER_PASSWORD_REGEX =
            "[A-Za-z0-9_%]+//?[A-Za-z0-9_%]*@";
    private static final String COLON_DELIMITER_USER_PASSWORD_REGEX =
            "([/|,])[A-Za-z0-9_%]+:?[A-Za-z0-9_%]*@";

    // strip the jdbc: prefix from the url. this leaves us with a url like
    // postgresql://<hostname>:<port>/<database_name>?params
    // we don't parse the URI here because different drivers use different connection
    // formats that aren't always amenable to how Java parses URIs. E.g., the oracle
    // driver format looks like oracle:<drivertype>:<user>/<password>@<database>
    // whereas postgres, mysql, and sqlserver use the scheme://hostname:port/db format.
    public static String sanitizeJdbcUrl(String jdbcUrl) {
        jdbcUrl = jdbcUrl.substring(5);
        return jdbcUrl
                .replaceAll(SLASH_DELIMITER_USER_PASSWORD_REGEX, "@")
                .replaceAll(COLON_DELIMITER_USER_PASSWORD_REGEX, "$1")
                .replaceAll("(?<=[?,;&:)=])\\(?(?i)(?:user|username|password)=[^;&,)]+(?:[;&;)]|$)", "");
    }
}