# scripts/import_mongo.R
# Import all provenance JSON files into MongoDB Atlas

library(mongolite)
library(jsonlite)

# =========================================
# 1) CONFIGURATION
# =========================================

mongo_url <- "mongodb+srv://carlaberardg_db_user:1234@carlaberard.mrx5yfu.mongodb.net/?retryWrites=true&w=majority&appName=CarlaBerard"

db_name <- "genomics_db"
collection_name <- "provenance"

drop_collection_first <- TRUE

project_dir <- "C:/Users/carla/OneDrive/Escritorio/URL/4º/2º/Data science/assignment 3"
json_folder <- file.path(project_dir, "data", "provenance_json")

# =========================================
# 2) CHECK JSON FOLDER
# =========================================

cat("Project directory:", project_dir, "\n")
cat("JSON folder:", json_folder, "\n")

if (!dir.exists(json_folder)) {
  stop("JSON folder does not exist: ", json_folder)
}

json_files <- list.files(
  path = json_folder,
  pattern = "\\.json$",
  full.names = TRUE
)

cat("JSON files found:", length(json_files), "\n")

if (length(json_files) == 0) {
  stop("No JSON files were found. Check the folder path: ", json_folder)
}

# =========================================
# 3) CONNECT TO MONGODB
# =========================================

conn <- tryCatch(
  mongo(
    collection = collection_name,
    db = db_name,
    url = mongo_url
  ),
  error = function(e) {
    stop(
      paste0(
        "Could not connect to MongoDB Atlas.\n",
        "Check the connection string, password, user permissions, and IP access.\n\n",
        "Original error: ", e$message
      )
    )
  }
)

tryCatch(
  {
    conn$count()
    cat("Connected to MongoDB Atlas successfully.\n")
  },
  error = function(e) {
    stop(
      paste0(
        "MongoDB connection was created but the database is not accessible.\n",
        "Check IP access list and database user privileges.\n\n",
        "Original error: ", e$message
      )
    )
  }
)

# =========================================
# 4) OPTIONAL: CLEAR COLLECTION
# =========================================

if (drop_collection_first) {
  conn$drop()
  cat("Existing collection dropped.\n")

  conn <- mongo(
    collection = collection_name,
    db = db_name,
    url = mongo_url
  )
}

# =========================================
# 5) IMPORT FILES
# =========================================

success_count <- 0
failed_count <- 0
failed_files <- character()

for (file in json_files) {
  tryCatch({
    doc <- fromJSON(
      file,
      simplifyVector = FALSE,
      simplifyDataFrame = FALSE,
      simplifyMatrix = FALSE
    )

    doc$source_file <- basename(file)

    conn$insert(doc)

    success_count <- success_count + 1
    cat("Imported:", basename(file), "\n")

  }, error = function(e) {
    failed_count <<- failed_count + 1
    failed_files <<- c(failed_files, basename(file))
    cat("FAILED:", basename(file), "->", e$message, "\n")
  })
}

# =========================================
# 6) VERIFY IMPORT
# =========================================

total_docs <- conn$count()

cat("\n========== IMPORT SUMMARY ==========\n")
cat("Successful imports:", success_count, "\n")
cat("Failed imports:    ", failed_count, "\n")
cat("Documents in MongoDB collection:", total_docs, "\n")

if (length(failed_files) > 0) {
  cat("\nFiles that failed to import:\n")
  print(failed_files)
}

cat("\nPreview of stored documents:\n")
print(conn$find(limit = 3))