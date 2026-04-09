# scripts/import_mongo.R
# Import all provenance JSON files from the project folder into MongoDB
# Each JSON file becomes one MongoDB document

# Install once if needed:
# install.packages(c("mongolite", "jsonlite"))

library(mongolite)
library(jsonlite)

# =========================
# 1) CONFIGURATION
# =========================

# Folder containing the 100 JSON files
json_folder <- "data/provenance_json"

# MongoDB Atlas connection string
# Replace with your real cluster URL
mongo_url <- "mongodb+srv://carlaberardg_db_user:4jvR6ufQHlPJkfpZ@cluster0.xxxxx.mongodb.net/"

# Database and collection names
db_name <- "genomics_db"
collection_name <- "provenance"

# If TRUE, deletes all existing documents in the collection before importing
drop_collection_first <- TRUE

# =========================
# 2) CONNECT TO MONGODB
# =========================

conn <- mongo(
  collection = collection_name,
  db = db_name,
  url = mongo_url
)

cat("Connected to MongoDB.\n")

# =========================
# 3) FIND JSON FILES
# =========================

json_files <- list.files(
  path = json_folder,
  pattern = "\\.json$",
  full.names = TRUE
)

cat("JSON files found:", length(json_files), "\n")

if (length(json_files) == 0) {
  stop("No JSON files were found. Check the folder path: ", json_folder)
}

# =========================
# 4) OPTIONAL: CLEAR COLLECTION
# =========================

if (drop_collection_first) {
  conn$drop()
  cat("Existing collection dropped.\n")
  
  # Reconnect after drop to ensure clean state
  conn <- mongo(
    collection = collection_name,
    db = db_name,
    url = mongo_url
  )
}

# =========================
# 5) IMPORT FILES
# =========================

success_count <- 0
failed_count <- 0
failed_files <- character()

for (file in json_files) {
  tryCatch({
    # Read raw JSON text
    json_text <- paste(readLines(file, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
    
    # Validate JSON before inserting
    fromJSON(json_text, simplifyVector = FALSE)
    
    # Insert one JSON file as one MongoDB document
    conn$insert(json_text)
    
    success_count <- success_count + 1
    cat("Imported:", basename(file), "\n")
    
  }, error = function(e) {
    failed_count <- failed_count + 1
    failed_files <<- c(failed_files, basename(file))
    cat("FAILED:", basename(file), "->", e$message, "\n")
  })
}

# =========================
# 6) VERIFY IMPORT
# =========================

total_docs <- conn$count()

cat("\n========== IMPORT SUMMARY ==========\n")
cat("Successful imports:", success_count, "\n")
cat("Failed imports:    ", failed_count, "\n")
cat("Documents in MongoDB collection:", total_docs, "\n")

if (length(failed_files) > 0) {
  cat("\nFiles that failed to import:\n")
  print(failed_files)
}

# Preview first few documents
cat("\nPreview of stored documents:\n")
print(conn$find(limit = 3))