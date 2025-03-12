#' Read Data from S3 Storage
#'
#' @param path Character string specifying the path to the file in S3
#' @param bucket Character string specifying the S3 bucket name
#' @param format Character string specifying the file format. One of "auto", "qs", "rds", "csv", or "parquet"
#' @return The data read from the S3 file in the specified format
#' @export
read_s3 <- function(path,
                    bucket,
                    format = c("auto", "qs", "rds", "csv", "parquet")) {
  format <- match.arg(format)
  if (format == "auto") {
    format <- tools::file_ext(path) |>
      switch(
        qs      = "qs",
        rds     = "rds",
        csv     = "csv",
        parquet = "parquet",
        stop("Unknown file format.")
      )
  }

  tryCatch(
    data <- aws.s3::get_object(
      object   = path,
      bucket   = bucket,
      base_url = "storjshare.io",
      region   = "gateway.us1",
      key      = Sys.getenv("AWS_ACCESS_KEY_ID"),
      secret   = Sys.getenv("AWS_SECRET_ACCESS_KEY")
    ),
    error = function(e) {
      stop(glue::glue("can't load {path} from S3."))
    }
  )

  if (format == "qs") {
    data |>
      qs::qdeserialize()
  } else if (format == "rds") {
    data |>
      rawConnection() |>
      gzcon() |>
      readRDS()
  } else if (format == "csv") {
    data |>
      rawConnection() |>
      readr::read_csv()
  } else if (format == "parquet") {
    data |>
      rawConnection() |>
      arrow::read_parquet()
  }
}

#' Write Data to S3 Storage
#'
#' @param data Data to be written to S3
#' @param bucket Character string specifying the S3 bucket name
#' @param format Character string specifying the file format. One of "qs", "rds", "csv", or "parquet"
#' @param name Optional character string specifying the output file name
#' @return Nothing. Writes data to S3 as a side effect.
#' @export
write_s3 <- function(data,
                     bucket,
                     format = c("qs", "rds", "csv", "parquet"),
                     name = NULL) {
  format <- match.arg(format)

  # return if data is empty
  if (is.null(data)) {
    cli::cli_inform("No data to save.")
    return()
  }

  # set file name and output path
  if (is.null(name)) {
    path_file <- paste0(deparse(substitute(data)), ".", format)
    path_s3 <- path_file
  } else {
    path_file <- fs::path_file(name)
    path_dir <- fs::path_dir(name)

    if (path_dir == ".") {
      path_s3 <- path_file
    } else {
      path_s3 <- paste0(path_dir, "/", path_file)
    }
  }

  # create temporary file in selected format
  file <- file.path(tempdir(), path_file)
  if (format == "qs") {
    qs::qsave(data, file)
  } else if (format == "rds") {
    saveRDS(data, file)
  } else if (format == "csv") {
    readr::write_csv(data, file, na = "")
  } else if (format == "parquet") {
    arrow::write_parquet(data, file)
  }

  tryCatch(
    aws.s3::put_object(
      file     = file,
      object   = path_s3,
      bucket   = bucket,
      base_url = "storjshare.io",
      region   = "gateway.us1",
      key      = Sys.getenv("AWS_ACCESS_KEY_ID"),
      secret   = Sys.getenv("AWS_SECRET_ACCESS_KEY")
    ),
    error = function(e) {
      stop(glue::glue("Can't save file {file} to S3."))
    }
  )
}

#' List Contents of an S3 Bucket
#'
#' @param bucket Character string specifying the S3 bucket name
#' @return A tibble containing information about files in the bucket
#' @export
list_bucket <- function(bucket) {
  aws.s3::get_bucket_df(
    bucket   = bucket,
    base_url = "storjshare.io",
    region   = "gateway.us1",
    key      = Sys.getenv("AWS_ACCESS_KEY_ID"),
    secret   = Sys.getenv("AWS_SECRET_ACCESS_KEY")
  ) |>
    dplyr::select(
      key      = Key,
      modified = LastModified,
      size     = Size
    ) |>
    dplyr::as_tibble()
}
