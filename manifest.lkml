project_name: "cetrogar-ga4-bigquery-v1"

## Connection Constants:
constant: GA4_CONNECTION {
  value: "bigquery_-_cetrogar_ga4"
  export: override_required
}

constant: GA4_SCHEMA {
  value: "analytics_288589538"
  export: override_optional
}

constant: GA4_TABLE_VARIABLE {
  value: "events_*"
  export: override_optional
}
constant: EVENT_COUNT {
  value: ""
  export: override_optional
}
constant: model_step_prediction {
  value: "60"
  #export: override_optional
}
constant: BQML_PARAMETER {
  value: "Yes"
  export: override_optional
}

constant: GA4_BQML_train_months {
  value: "12"
  # export: override_optional
}

constant: GA4_BQML_test_months {
  value: "3"
  # export: override_optional
}

constant: GA4_BQML_future_synth_months {
  value: "12"
  # export: override_optional
}
