# line_graph_cumulative ---------------------------------------------------------

#' Cumulative line chart
#'
#' @param data A data frame.
#' @param display One of "numbers" or "percent".
#' @param xaxis Column name for the date x-axis.
#' @param color Column name for line color grouping, or "none".
#' @param facet Column name for faceting, or "none".
#' @param unit Label for count units shown in tooltips.
#' @return A plotly object.
#' @export
line_graph_cumulative <- function(data, display, xaxis, color, facet, unit = "units") {
  ## -- prepare data ------------------------------------------------------------
  data_plot <- data |>
    dplyr::group_by(dplyr::across(dplyr::any_of(c(color, facet)))) |>
    dplyr::count(dplyr::across(dplyr::any_of(xaxis))) |>
    dplyr::mutate(
      total       = cumsum(n),
      grand_total = max(total)
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      percent = round(100 * total / grand_total, 1),
      y       = if (display == "numbers") total else percent
    ) |>
    line_add_label(xaxis, color, facet, unit)

  ## -- base ggplot -------------------------------------------------------------
  if (color != "none") {
    p <- ggplot2::ggplot(
      data_plot,
      ggplot2::aes(
        x     = .data[[xaxis]],
        y     = y,
        color = .data[[color]],
        group = .data[[color]],
        text  = label
      )
    )
  } else {
    p <- ggplot2::ggplot(
      data_plot,
      ggplot2::aes(x = .data[[xaxis]], y = y, group = 1, text = label)
    )
  }

  if (color != "none") p <- p + ggplot2::scale_color_viridis_d()

  if (facet == "none") {
    plotly_margin_right <- 0
  } else {
    plotly_margin_right <- 5
    p <- p +
      ggplot2::facet_grid(
        rows     = ggplot2::vars(.data[[facet]]),
        scales   = "free_y",
        labeller = ggplot2::label_wrap_gen(width = 15)
      )
  }

  if (display != "numbers") {
    p <- p + ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1, scale = 1))
  }

  p <- p +
    ggplot2::geom_line(linewidth = 0.75) +
    ggplot2::scale_x_date(date_breaks = "3 month", date_labels = "%Y %b") +
    ggplot2::labs(x = "", y = "") +
    ggplot2::theme_minimal(base_size = 13, base_family = "Inter") +
    ggplot2::theme(
      strip.text.y  = ggplot2::element_text(angle = 0),
      plot.margin   = ggplot2::margin(0, plotly_margin_right, 0, 0, "cm"),
      panel.spacing = ggplot2::unit(2, "points"),
      axis.text.x   = ggplot2::element_text(angle = 40)
    )

  ## -- ggplotly ----------------------------------------------------------------
  plotly::ggplotly(p, tooltip = "text") |>
    dashboard_plotly_theme(
      hovermode = "x unified",
      legend = list(
        orientation = "h",
        xanchor     = "center",
        yanchor     = "bottom",
        x           = 0.5,
        y           = 1.025,
        title       = list(text = "")
      )
    )
}


## -- helpers ------------------------------------------------------------------

#' @noRd
line_add_label <- function(data, xaxis, color, facet, unit) {
  if (color == "none") {
    data <- data |>
      dplyr::mutate(
        text_label = glue::glue(
          "{format(.data[[xaxis]], '%b %d, %Y')}:<br> <b>{format(total, big.mark = ',')}</b> {unit} \U2022 {scales::label_percent(accuracy = 0.1)(percent/100)}"
        )
      )
  } else {
    data <- data |>
      dplyr::mutate(
        text_label = glue::glue(
          "<b>{.data[[color]]}</b> \U2022 {format(.data[[xaxis]], '%b %d, %Y')}:<br> <b>{format(total, big.mark = ',')}</b> {unit} \U2022 {scales::label_percent(accuracy = 0.1)(percent/100)}"
        )
      )
  }

  if (facet != "none") {
    data <- data |>
      dplyr::group_by(.data[[xaxis]], .data[[facet]]) |>
      dplyr::mutate(index = dplyr::row_number()) |>
      dplyr::ungroup() |>
      dplyr::mutate(
        text_header = ifelse(
          index == 1,
          glue::glue("<b>{.data[[facet]]}</b><br>"),
          ""
        ),
        label = paste(text_header, text_label)
      )
  } else {
    data |> dplyr::mutate(label = text_label)
  }
}
