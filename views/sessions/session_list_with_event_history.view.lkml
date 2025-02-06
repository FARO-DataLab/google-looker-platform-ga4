include: "/views/event_data_dimensions/event_funnel.view"
include: "/views/event_data_dimensions/page_funnel.view"
###########################
# 01: SESSION LIST WITH EVENT HISTORY
# This is the backbone of the block.
# In this query, we parse the date from the table name
# And sessionalize all the events with the sl_key
############################
view: session_list_with_event_history {
  derived_table: {
    datagroup_trigger: ga4_main_datagroup
    partition_keys: ["session_date"]
    cluster_keys: ["sl_key","user_id","session_date"]
    increment_key: "session_date"
    increment_offset: 0
    sql: WITH deduplicated_events AS (
          SELECT DISTINCT  -- Añadimos una CTE con DISTINCT para eliminar duplicados
            timestamp(SAFE.PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'(\d{8})'))) as session_date,
            (select value.int_value from UNNEST(events.event_params) where key = "ga_session_id") as ga_session_id,
            (select value.int_value from UNNEST(events.event_params) where key = "ga_session_number") as ga_session_number,
            events.user_pseudo_id,
            events.event_date,
            events.event_timestamp,
            events.event_name,
            events.event_params,
            events.event_previous_timestamp,
            events.event_value_in_usd,
            events.event_bundle_sequence_id,
            events.event_server_timestamp_offset,
            events.user_id,
            events.user_properties,
            events.user_first_touch_timestamp,
            events.user_ltv,
            events.device,
            events.geo,
            events.app_info,
            events.traffic_source,
            events.stream_id,
            events.platform,
            events.event_dimensions,
            events.ecommerce,
            events.items
          FROM `@{GA4_SCHEMA}.@{GA4_TABLE_VARIABLE}` events
          WHERE {% incrementcondition %} timestamp(PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_TABLE_SUFFIX,r'[0-9]+'))) {%  endincrementcondition %}
        )
        select
          session_date,
          ga_session_id,
          ga_session_number,
          user_pseudo_id,
          -- unique key for session:
          session_date||ga_session_id||ga_session_number||user_pseudo_id as sl_key,
          row_number() over (partition by session_date||ga_session_id||ga_session_number||user_pseudo_id order by event_timestamp) event_rank,
          -- resto de los campos igual que antes pero usando deduplicated_events en lugar de events directamente
          (TIMESTAMP_DIFF(TIMESTAMP_MICROS(LEAD(event_timestamp) OVER (PARTITION BY session_date||ga_session_id||ga_session_number||user_pseudo_id ORDER BY event_timestamp asc))
          ,TIMESTAMP_MICROS(event_timestamp),second)/86400.0) time_to_next_event,
          case when event_name = 'page_view' then row_number() over (partition by session_date||ga_session_id||ga_session_number||user_pseudo_id, case when event_name = 'page_view' then true else false end order by event_timestamp)
          else 0 end as page_view_rank,
          case when event_name = 'page_view' then row_number() over (partition by session_date||ga_session_id||ga_session_number||user_pseudo_id, case when event_name = 'page_view' then true else false end order by event_timestamp desc)
          else 0 end as page_view_reverse_rank,
          case when event_name = 'page_view' then (TIMESTAMP_DIFF(TIMESTAMP_MICROS(LEAD(event_timestamp) OVER (PARTITION BY session_date||ga_session_id||ga_session_number||user_pseudo_id, case when event_name = 'page_view' then true else false end ORDER BY event_timestamp asc))
          ,TIMESTAMP_MICROS(event_timestamp),second)/86400.0) else null end as time_to_next_page,
          event_date,
          event_timestamp,
          event_name,
          event_params,
          event_previous_timestamp,
          event_value_in_usd,
          event_bundle_sequence_id,
          event_server_timestamp_offset,
          user_id,
          user_properties,
          user_first_touch_timestamp,
          user_ltv,
          device,
          geo,
          app_info,
          traffic_source,
          stream_id,
          platform,
          event_dimensions,
          ecommerce,
          ARRAY(select as STRUCT it.* EXCEPT(item_params) from unnest(items) as it) as items
        from deduplicated_events
    ;;
  }
  dimension: session_date {
    type: date
    hidden: yes
    sql: ${TABLE}.session_date;;
  }
  parameter: bqm_enabled {
    type: unquoted
    default_value: "@{BQML_PARAMETER}"
  }
}
