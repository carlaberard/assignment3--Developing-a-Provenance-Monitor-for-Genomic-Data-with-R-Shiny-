# app.R
# Provenance Monitor for Genomic Data with R Shiny
# Data source: MongoDB Atlas only

# Install once if needed:
# install.packages(c("shiny", "DT", "jsonlite", "dplyr", "lubridate", "ggplot2", "mongolite"))

library(shiny)
library(DT)
library(jsonlite)
library(dplyr)
library(lubridate)
library(ggplot2)
library(mongolite)

# =========================================
# 1) MONGODB CONFIGURATION
# =========================================

mongo_url <- "mongodb+srv://carlaberardg_db_user:1234@carlaberard.mrx5yfu.mongodb.net/?retryWrites=true&w=majority&appName=CarlaBerard"
db_name <- "genomics_db"
collection_name <- "provenance"

node_colors <- c(
  "cresselia" = "#3b82f6",
  "gengar"    = "#8b5cf6",
  "snorlax"   = "#10b981",
  "alakazam"  = "#f59e0b"
)

# =========================================
# 2) HELPERS
# =========================================

safe_chr <- function(x, default = NA_character_) {
  if (is.null(x) || length(x) == 0) return(default)
  as.character(x[[1]])
}

safe_num <- function(x, default = NA_real_) {
  if (is.null(x) || length(x) == 0) return(default)
  suppressWarnings(as.numeric(x[[1]]))
}

extract_user <- function(wasAssociatedWith) {
  if (is.null(wasAssociatedWith) || length(wasAssociatedWith) == 0) {
    return(NA_character_)
  }

  for (item in wasAssociatedWith) {
    type_value <- item[["@type"]]
    if (!is.null(type_value) && "Person" %in% unlist(type_value)) {
      return(safe_chr(item$label))
    }
  }

  NA_character_
}

extract_seqfu_version <- function(wasAssociatedWith) {
  if (is.null(wasAssociatedWith) || length(wasAssociatedWith) == 0) {
    return(NA_character_)
  }

  for (item in wasAssociatedWith) {
    label_value <- item$label
    if (!is.null(label_value) && any(grepl("seqfu", unlist(label_value), ignore.case = TRUE))) {
      return(safe_chr(item$version))
    }
  }

  NA_character_
}

extract_generated_item <- function(generated, label_name) {
  if (is.null(generated) || length(generated) == 0) return(NULL)

  for (item in generated) {
    label_value <- item$label
    if (!is.null(label_value) && label_name %in% unlist(label_value)) {
      return(item)
    }
  }

  NULL
}

parse_sha_status <- function(generated) {
  item <- extract_generated_item(generated, "SHA256 Verification")
  if (is.null(item)) return(NA_character_)

  value <- tolower(safe_chr(item$value, ""))
  if (grepl("failed|error|mismatch|does not match", value)) {
    return("Failed")
  }
  "Passed"
}

parse_seqfu_status <- function(generated) {
  item <- extract_generated_item(generated, "Seqfu Verification")
  if (is.null(item)) return(NA_character_)

  value <- tolower(safe_chr(item$value, ""))
  if (grepl("^ok|\\bok\\b", value)) {
    return("Passed")
  }
  "Failed"
}

extract_total_size_bytes <- function(generated) {
  item <- extract_generated_item(generated, "FASTQ Files")
  if (is.null(item)) return(NA_real_)
  safe_num(item$totalSizeBytes)
}

extract_file_count <- function(generated) {
  item <- extract_generated_item(generated, "FASTQ Files")
  if (is.null(item)) return(NA_real_)
  safe_num(item$fileCount)
}

connect_mongo <- function() {
  mongo(
    collection = collection_name,
    db = db_name,
    url = mongo_url
  )
}

read_mongo_documents <- function() {
  conn <- connect_mongo()
  raw_docs <- conn$find('{}')

  if (nrow(raw_docs) == 0) {
    return(list())
  }

  docs <- lapply(seq_len(nrow(raw_docs)), function(i) {
    row_list <- as.list(raw_docs[i, , drop = FALSE])

    if ("_id" %in% names(row_list)) {
      row_list[["_id"]] <- NULL
    }

    fromJSON(
      toJSON(row_list, auto_unbox = TRUE, null = "null"),
      simplifyVector = FALSE,
      simplifyDataFrame = FALSE,
      simplifyMatrix = FALSE
    )
  })

  docs
}

docs_to_table <- function(docs) {
  if (length(docs) == 0) return(data.frame())

  rows <- lapply(docs, function(doc) {
    start_time <- ymd_hms(safe_chr(doc$startTime), tz = "UTC", quiet = TRUE)
    end_time   <- ymd_hms(safe_chr(doc$endTime), tz = "UTC", quiet = TRUE)

    total_size_bytes <- extract_total_size_bytes(doc$generated)
    duration_mins <- as.numeric(difftime(end_time, start_time, units = "mins"))

    data.frame(
      prov_id = safe_chr(doc[["@id"]]),
      source_file = safe_chr(doc$source_file),
      label = safe_chr(doc$label),
      executionNode = safe_chr(doc$executionNode),
      user = extract_user(doc$wasAssociatedWith),
      seqfu_version = extract_seqfu_version(doc$wasAssociatedWith),
      startTime = safe_chr(doc$startTime),
      endTime = safe_chr(doc$endTime),
      start_date = as.Date(start_time),
      duration_min = round(duration_mins, 2),
      sha256_status = parse_sha_status(doc$generated),
      seqfu_status = parse_seqfu_status(doc$generated),
      total_size_bytes = total_size_bytes,
      total_size_gb = round(total_size_bytes / (1024^3), 2),
      fileCount = extract_file_count(doc$generated),
      stringsAsFactors = FALSE
    )
  })

  bind_rows(rows)
}

match_integrity_filter <- function(df, mode) {
  if (mode == "All") return(rep(TRUE, nrow(df)))
  if (mode == "All passed") return(df$sha256_status == "Passed" & df$seqfu_status == "Passed")
  if (mode == "Any failure") return(df$sha256_status == "Failed" | df$seqfu_status == "Failed")
  if (mode == "SHA256 failed") return(df$sha256_status == "Failed")
  if (mode == "Seqfu failed") return(df$seqfu_status == "Failed")
  rep(TRUE, nrow(df))
}

build_display_table <- function(df) {
  df %>%
    mutate(
      run_status = ifelse(
        sha256_status == "Failed" | seqfu_status == "Failed",
        "Failed",
        "Passed"
      )
    ) %>%
    select(
      prov_id,
      source_file,
      label,
      executionNode,
      user,
      seqfu_version,
      startTime,
      endTime,
      duration_min,
      sha256_status,
      seqfu_status,
      total_size_gb,
      fileCount,
      run_status
    )
}

metric_card <- function(title, output_id, accent = "#4f46e5") {
  div(
    class = "metric-card",
    style = paste0("border-top: 4px solid ", accent, ";"),
    div(class = "metric-title", title),
    div(class = "metric-value", textOutput(output_id, inline = TRUE))
  )
}

panel_card <- function(title, ...) {
  div(
    class = "panel-card",
    div(class = "panel-title", title),
    ...
  )
}

# =========================================
# 3) INITIAL LOAD FROM MONGODB
# =========================================

all_docs <- tryCatch(
  read_mongo_documents(),
  error = function(e) {
    stop(
      paste0(
        "Error connecting to MongoDB Atlas.\n",
        "Check the connection string, password, database user permissions, and network access.\n\n",
        "Original error: ", e$message
      )
    )
  }
)

all_table <- docs_to_table(all_docs)

if (nrow(all_table) == 0) {
  stop(
    paste0(
      "No documents found in MongoDB collection '",
      collection_name,
      "' in database '",
      db_name,
      "'. Run scripts/import_mongo.R first."
    )
  )
}

# =========================================
# 4) UI
# =========================================

ui <- fluidPage(
  tags$head(
    tags$title("Provenance Monitor"),
    tags$style(HTML("
      body {
        background: #f3f6fb;
        font-family: 'Segoe UI', Arial, sans-serif;
        color: #1f2937;
      }
      .container-fluid {
        padding-left: 22px;
        padding-right: 22px;
      }
      .app-header {
        background: linear-gradient(135deg, #1e293b, #334155);
        color: white;
        border-radius: 18px;
        padding: 24px 28px;
        margin: 18px 0 20px 0;
        box-shadow: 0 10px 30px rgba(15, 23, 42, 0.18);
      }
      .app-title {
        font-size: 34px;
        font-weight: 700;
        margin-bottom: 6px;
        line-height: 1.15;
      }
      .app-subtitle {
        font-size: 15px;
        opacity: 0.9;
      }
      .sidebar-card, .panel-card, .metric-card {
        background: white;
        border-radius: 16px;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
        border: 1px solid #e5e7eb;
      }
      .sidebar-card {
        padding: 18px;
        margin-bottom: 18px;
      }
      .panel-card {
        padding: 18px;
        margin-bottom: 18px;
      }
      .panel-title {
        font-size: 18px;
        font-weight: 700;
        margin-bottom: 14px;
        color: #111827;
      }
      .metric-card {
        padding: 16px 18px;
        min-height: 118px;
        margin-bottom: 18px;
      }
      .metric-title {
        font-size: 14px;
        font-weight: 600;
        color: #6b7280;
        margin-bottom: 10px;
      }
      .metric-value {
        font-size: 30px;
        font-weight: 800;
        color: #111827;
      }
      .filter-title {
        font-size: 18px;
        font-weight: 700;
        margin-bottom: 14px;
      }
      .control-label {
        font-weight: 600;
        color: #374151;
      }
      .form-control, .selectize-input {
        border-radius: 10px !important;
        border: 1px solid #d1d5db !important;
        box-shadow: none !important;
      }
      .btn, .btn-default {
        border-radius: 10px !important;
        font-weight: 600;
      }
      .btn-primary, .btn-default {
        background: #2563eb !important;
        color: white !important;
        border: none !important;
      }
      .nav-tabs {
        border-bottom: none;
        margin-bottom: 10px;
      }
      .nav-tabs > li > a {
        border-radius: 10px !important;
        border: none !important;
        background: #e5e7eb;
        color: #374151;
        margin-right: 8px;
        font-weight: 600;
      }
      .nav-tabs > li.active > a,
      .nav-tabs > li.active > a:hover,
      .nav-tabs > li.active > a:focus {
        background: #2563eb !important;
        color: white !important;
      }
      .dataTables_wrapper .dataTables_filter input,
      .dataTables_wrapper .dataTables_length select {
        border-radius: 8px;
        border: 1px solid #d1d5db;
      }
      table.dataTable tbody tr.selected {
        background-color: #dbeafe !important;
      }
      pre {
        white-space: pre-wrap;
        word-break: break-word;
        font-size: 12px;
        line-height: 1.45;
        margin-bottom: 0;
      }
      .schema-box {
        max-height: 480px;
        overflow-y: auto;
        background: #0f172a;
        color: #e5e7eb;
        border-radius: 12px;
        padding: 14px;
      }
    "))
  ),

  div(
    class = "app-header",
    div(class = "app-title", "Provenance Monitor for Genomic Data"),
    div(
      class = "app-subtitle",
      "Interactive dashboard for system health, execution efficiency, throughput, and full provenance inspection from MongoDB Atlas."
    )
  ),

  fluidRow(
    column(
      width = 3,
      div(
        class = "sidebar-card",
        div(class = "filter-title", "Filters"),

        selectInput(
          "node_filter",
          "Execution node",
          choices = c("All", sort(unique(all_table$executionNode))),
          selected = "All",
          multiple = TRUE
        ),

        selectInput(
          "integrity_filter",
          "Integrity status",
          choices = c("All", "All passed", "Any failure", "SHA256 failed", "Seqfu failed"),
          selected = "All"
        ),

        dateRangeInput(
          "date_filter",
          "Start date range",
          start = min(all_table$start_date, na.rm = TRUE),
          end = max(all_table$start_date, na.rm = TRUE),
          min = min(all_table$start_date, na.rm = TRUE),
          max = max(all_table$start_date, na.rm = TRUE)
        ),

        actionButton("refresh_data", "Refresh data from MongoDB")
      )
    ),

    column(
      width = 9,

      fluidRow(
        column(3, metric_card("Records", "records_value", "#2563eb")),
        column(3, metric_card("Integrity Failures", "failed_checks_value", "#f59e0b")),
        column(3, metric_card("Total Data Processed (GB)", "throughput_value", "#10b981")),
        column(3, metric_card("Average Processing Time (min)", "avg_duration_value", "#8b5cf6"))
      ),

      tabsetPanel(
        tabPanel(
          "Overview",
          br(),
          fluidRow(
            column(
              6,
              panel_card(
                "Average Duration by Execution Node",
                plotOutput("duration_plot", height = 320)
              )
            ),
            column(
              6,
              panel_card(
                "Throughput by Execution Node",
                plotOutput("throughput_plot", height = 320)
              )
            )
          ),
          fluidRow(
            column(
              12,
              panel_card(
                "Integrity Status Distribution",
                plotOutput("failure_plot", height = 320)
              )
            )
          )
        ),

        tabPanel(
          "Records",
          br(),
          panel_card(
            "Provenance Records Table",
            DTOutput("prov_table")
          )
        ),

        tabPanel(
          "Schema Viewer",
          br(),
          panel_card(
            "Full Provenance Schema",
            tags$p("Select a row in the Records table to display the complete JSON schema here."),
            div(class = "schema-box", textOutput("full_schema"))
          )
        )
      )
    )
  )
)

# =========================================
# 5) SERVER
# =========================================

server <- function(input, output, session) {

  docs_rv <- reactiveVal(all_docs)
  table_rv <- reactiveVal(all_table)

  observeEvent(input$refresh_data, {
    docs <- tryCatch(
      read_mongo_documents(),
      error = function(e) {
        showNotification(
          paste("MongoDB refresh failed:", e$message),
          type = "error",
          duration = 8
        )
        return(NULL)
      }
    )

    if (!is.null(docs) && length(docs) > 0) {
      tbl <- docs_to_table(docs)
      docs_rv(docs)
      table_rv(tbl)
      showNotification("Data refreshed successfully from MongoDB.", type = "message", duration = 4)
    }
  })

  filtered_table <- reactive({
    df <- table_rv()

    if (!("All" %in% input$node_filter)) {
      df <- df %>% filter(executionNode %in% input$node_filter)
    }

    keep_integrity <- match_integrity_filter(df, input$integrity_filter)
    df <- df[keep_integrity, , drop = FALSE]

    df <- df %>%
      filter(
        start_date >= input$date_filter[1],
        start_date <= input$date_filter[2]
      )

    df
  })

  output$records_value <- renderText({
    nrow(filtered_table())
  })

  output$failed_checks_value <- renderText({
    df <- filtered_table()
    sum(df$sha256_status == "Failed" | df$seqfu_status == "Failed", na.rm = TRUE)
  })

  output$throughput_value <- renderText({
    df <- filtered_table()
    round(sum(df$total_size_gb, na.rm = TRUE), 2)
  })

  output$avg_duration_value <- renderText({
    df <- filtered_table()
    round(mean(df$duration_min, na.rm = TRUE), 2)
  })

  output$duration_plot <- renderPlot({
    df <- filtered_table() %>%
      group_by(executionNode) %>%
      summarise(avg_duration = mean(duration_min, na.rm = TRUE), .groups = "drop")

    shiny::validate(
      shiny::need(nrow(df) > 0, "No data available for this filter selection.")
    )

    ggplot(df, aes(x = executionNode, y = avg_duration, fill = executionNode)) +
      geom_col(width = 0.7) +
      scale_fill_manual(values = node_colors, drop = FALSE) +
      labs(
        x = NULL,
        y = "Minutes",
        fill = "Execution Node"
      ) +
      theme_minimal(base_size = 13) +
      theme(
        plot.background = element_rect(fill = "white", colour = NA),
        panel.grid.minor = element_blank(),
        axis.text.x = element_text(angle = 0, hjust = 0.5)
      )
  })

  output$throughput_plot <- renderPlot({
    df <- filtered_table() %>%
      group_by(executionNode) %>%
      summarise(total_gb = sum(total_size_gb, na.rm = TRUE), .groups = "drop")

    shiny::validate(
      shiny::need(nrow(df) > 0, "No data available for this filter selection.")
    )

    ggplot(df, aes(x = executionNode, y = total_gb, fill = executionNode)) +
      geom_col(width = 0.7) +
      scale_fill_manual(values = node_colors, drop = FALSE) +
      labs(
        x = NULL,
        y = "GB",
        fill = "Execution Node"
      ) +
      theme_minimal(base_size = 13) +
      theme(
        plot.background = element_rect(fill = "white", colour = NA),
        panel.grid.minor = element_blank(),
        axis.text.x = element_text(angle = 0, hjust = 0.5)
      )
  })

  output$failure_plot <- renderPlot({
    df <- filtered_table() %>%
      mutate(
        run_status = ifelse(
          sha256_status == "Failed" | seqfu_status == "Failed",
          "Failed",
          "Passed"
        )
      ) %>%
      count(run_status, name = "n")

    shiny::validate(
      shiny::need(nrow(df) > 0, "No data available for this filter selection.")
    )

    ggplot(df, aes(x = run_status, y = n, fill = run_status)) +
      geom_col(width = 0.55, show.legend = FALSE) +
      scale_fill_manual(values = c(
        "Passed" = "#10b981",
        "Failed" = "#ef4444"
      )) +
      labs(
        x = NULL,
        y = "Runs"
      ) +
      theme_minimal(base_size = 13) +
      theme(
        plot.background = element_rect(fill = "white", colour = NA),
        panel.grid.minor = element_blank()
      )
  })

  output$prov_table <- renderDT({
    df <- build_display_table(filtered_table())

    datatable(
      df,
      selection = "single",
      rownames = FALSE,
      filter = "top",
      class = "compact stripe hover",
      options = list(
        pageLength = 10,
        scrollX = TRUE,
        autoWidth = TRUE,
        rowCallback = JS(
          "function(row, data) {",
          "  if (data[13] === 'Failed') {",
          "    $('td', row).css({'background-color': '#fff1f2'});",
          "  }",
          "}"
        )
      )
    )
  })

  output$full_schema <- renderText({
    selected <- input$prov_table_rows_selected
    shown_df <- build_display_table(filtered_table())
    docs <- docs_rv()

    if (length(selected) == 0 || nrow(shown_df) == 0) {
      return("No row selected.")
    }

    selected_id <- shown_df$prov_id[selected[1]]

    idx <- which(vapply(
      docs,
      function(doc) identical(safe_chr(doc[["@id"]]), selected_id),
      logical(1)
    ))

    if (length(idx) == 0) {
      return("Selected document not found.")
    }

    toJSON(
      docs[[idx[1]]],
      pretty = TRUE,
      auto_unbox = TRUE,
      null = "null"
    )
  })
}

# =========================================
# 6) RUN
# =========================================

shinyApp(ui = ui, server = server)