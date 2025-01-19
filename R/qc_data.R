#' Data Quality Control Function
#'
#' Validates the provided data frame by checking its format and ensuring that required columns exist.
#'
#' @param data A data frame to validate.
#' @param required_columns A character vector of column names that must exist in the data. Default is NULL (no required columns).
#' @param alert Logical. If TRUE, displays an alert using shinyWidgets if a validation fails. Defaults to the result of `shiny::isRunning()`.
#'
#' @return Logical. Returns TRUE if the data frame passes all checks; otherwise, FALSE.
#'
#' @examples
#' df <- data.frame(a = 1:5, b = 6:10)
#' qc_data(df, required_columns = c("a", "b"))
#' qc_data(df, required_columns = c("a", "c"))
qc_data <- function(data, required_columns = NULL, alert = shiny::isRunning()) {

  # Check if input is a valid data frame
  if (!chk::vld_data(data)) {
    msg <- "Data is not a data frame."
    logger::log_error(msg)

    if (alert) {
      shinyWidgets::show_alert(
        title = "Invalid Data Format",
        text = msg,
        type = "error"
      )
    }
    return(FALSE)
  }

  # Check if all required columns exist in the data frame
  if (!is.null(required_columns)) {
    missing_columns <- setdiff(required_columns, names(data))

    if (length(missing_columns) > 0) {
      msg <- glue::glue(
        "The following required columns are missing: {paste(missing_columns, collapse = ', ')}"
      )
      logger::log_error(msg)

      if (alert) {
        shinyWidgets::show_alert(
          title = "Missing Required Columns",
          text = msg,
          type = "error"
        )
      }
      return(FALSE)
    }
  }

  # All checks passed
  TRUE
}
