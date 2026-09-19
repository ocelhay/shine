# barplot_interactive -----------------------------------------------------------

#' Interactive bar chart
#'
#' @param data A data frame.
#' @param display One of "numbers" or "percent".
#' @param xaxis Column name for x-axis grouping.
#' @param color Column name for fill color grouping, or "none".
#' @param facet Column name for faceting, or "none".
#' @param facet_total Logical; highlight the first facet panel as a total.
#' @param count Logical; if TRUE, count rows before plotting.
#' @param flip Logical; flip coordinates.
#' @param unit Label for count units shown in tooltips.
#' @param order One of "count" or "default".
#' @param palette Optional named character vector of fill colours, keyed by the
#'   levels of `color`. Use it when the levels carry a meaning the reader is
#'   expected to know (AWaRe classes, S/I/R); leave it `NULL` for plain
#'   categories, which keep the default viridis scale.
#' @return A plotly object.
#' @export
barplot_interactive <- function(
  data,
  display,
  xaxis,
  color,
  facet,
  facet_total,
  count = TRUE,
  flip = FALSE,
  unit = "units",
  order,
  palette = NULL
) {
  ## -- prepare data ------------------------------------------------------------
  if (count) {
    data <- data |>
      dplyr::count(dplyr::across(dplyr::any_of(c(xaxis, color, facet)))) |>
      tidyr::complete(
        !!!rlang::syms(setdiff(c(xaxis, color, facet), "none")),
        fill = list(n = 0)
      )
  }
  data_plot <- data |>
    dplyr::group_by(dplyr::across(dplyr::any_of(c(xaxis, facet)))) |>
    dplyr::mutate(
      none = 0,
      percent = round(n / sum(n) * 100, 2)
    ) |>
    dplyr::ungroup() |>
    barplot_add_label(xaxis, color, facet, display, unit)

  ## -- base ggplot -------------------------------------------------------------
  if (display == "numbers") {
    if (flip | order == "count") {
      p <- ggplot2::ggplot(
        data_plot,
        ggplot2::aes(
          x = reorder(.data[[xaxis]], -n),
          y = n,
          fill = .data[[color]],
          text = .data[["text_label"]]
        )
      )
    } else {
      p <- ggplot2::ggplot(
        data_plot,
        ggplot2::aes(
          x = .data[[xaxis]],
          y = n,
          fill = .data[[color]],
          text = .data[["text_label"]]
        )
      )
    }
    p <- p +
      ggplot2::scale_y_continuous(
        breaks = function(x) {
          unique(floor(pretty(seq(min(x), (max(x) + 1) * 1.1))))
        }
      )
  } else {
    if (flip) {
      p <- ggplot2::ggplot(
        data_plot,
        ggplot2::aes(
          x = reorder(.data[[xaxis]], -n),
          y = percent,
          fill = .data[[color]],
          text = .data[["text_label"]]
        )
      )
    } else {
      p <- ggplot2::ggplot(
        data_plot,
        ggplot2::aes(
          x = .data[[xaxis]],
          y = percent,
          fill = .data[[color]],
          text = .data[["text_label"]]
        )
      )
    }
    p <- p +
      ggplot2::scale_y_continuous(labels = scales::percent_format(scale = 1))
  }

  p <- p +
    ggplot2::geom_bar(
      position = "stack",
      stat = "identity",
      width = barplot_optimal_width(data_plot, xaxis),
      show.legend = (color != "none")
    )

  if (flip) {
    p <- p + ggplot2::coord_flip()
  }

  if (facet == "none") {
    plotly_margin_right <- 0
  } else {
    plotly_margin_right <- 5
    p <- p +
      ggplot2::facet_grid(
        rows = ggplot2::vars(.data[[facet]]),
        scales = "free",
        labeller = ggplot2::label_wrap_gen(width = 15)
      )
  }

  if (color != "none") {
    p <- p +
      if (is.null(palette)) {
        ggplot2::scale_fill_viridis_d()
      } else {
        # drop = FALSE so an empty level keeps its colour and its legend entry,
        # rather than the colours shifting when a filter empties one out.
        #
        # na.value is paler than any sensible category colour on purpose: NA
        # here means the value was never recorded, which must not be mistaken
        # for a category the palette deliberately greys out. ggplot2's default
        # of grey50 collides with exactly that.
        ggplot2::scale_fill_manual(
          values = palette,
          drop = FALSE,
          na.value = "#d9d9d9"
        )
      }
  }

  p <- p +
    ggplot2::scale_x_discrete(
      labels = function(x) stringr::str_wrap(x, width = (30 + 50 * flip))
    ) +
    ggplot2::labs(x = "", y = "") +
    ggplot2::theme_minimal(base_size = 13, base_family = "Inter") +
    ggplot2::theme(
      strip.text.y = ggplot2::element_text(angle = 0),
      plot.margin = ggplot2::margin(0, plotly_margin_right, 0, 0, "cm"),
      panel.spacing = ggplot2::unit(2, "points"),
      axis.text.x = ggplot2::element_text(angle = ifelse(flip, 0, 40))
    )

  ## -- ggplotly ----------------------------------------------------------------
  pp <- plotly::ggplotly(p, tooltip = "text") |>
    dashboard_plotly_theme(
      hovermode = ifelse(flip, "y", "x unified"),
      legend = list(
        orientation = "h",
        xanchor = "center",
        yanchor = "bottom",
        x = 0.5,
        y = 1.025,
        title = list(text = "")
      )
    )

  if (facet_total) {
    pp$x$layout$annotations[[1]]$text <- "<b>Total</b>"
    pp$x$layout$annotations[[1]]$font$color <- "rgb(179, 0, 0)"
    pp$x$layout$shapes[[1]]$line <- list(color = "rgb(179, 0, 0)", width = 3)
  }
  return(pp)
}


## -- helpers ------------------------------------------------------------------

#' @noRd
barplot_add_label <- function(data, xaxis, color, facet, display, unit) {
  if (color == "none") {
    data <- data |>
      dplyr::mutate(
        text_label = glue::glue(
          "<b>{.data[[xaxis]]}</b>: {format(n, big.mark = ',')} {unit}"
        )
      )
  } else {
    data <- data |>
      dplyr::mutate(
        text_label = glue::glue(
          "<b><i>{.data[[color]]}</i>\U2022{.data[[xaxis]]}</b>: {percent}% - {format(n, big.mark = ',')} {unit}"
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
        text_label = paste(text_header, text_label)
      )
  }
  data
}


#' @noRd
barplot_optimal_width <- function(data, xaxis) {
  nb_bars <- data |>
    dplyr::count(dplyr::across(dplyr::any_of(xaxis))) |>
    nrow()

  dplyr::case_when(
    nb_bars == 1 ~ 0.1,
    nb_bars <= 5 ~ 0.3,
    nb_bars <= 8 ~ 0.4,
    nb_bars <= 10 ~ 0.5,
    nb_bars <= 20 ~ 0.47,
    .default = 0.9
  )
}
