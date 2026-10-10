# Load required packages
library(tidyverse)
library(gtsummary)
library(gt)

# Check for command line flag to filter by diagnosis year
args <- commandArgs(trailingOnly = TRUE)
five_year_diagnosis <- "--five_year_diagnosis" %in% args

# Repeatable questionnaire tables not currently supported (family health history)
manifest <- read.csv("questionnaire_manifest.csv")
setwd("Data/")
patient_profile_eligibility <- read_csv("patient_profile_eligibility.csv", show_col_types = FALSE)

# Read in each file listed in the manifest
manifest <- manifest %>%
  mutate(data = map(file_path, ~ read_csv(.x, show_col_types = FALSE)))

if (five_year_diagnosis) {
  
  # Filter eligibility to participants diagnosed since 2020
  patient_profile_eligibility <- patient_profile_eligibility %>%
    filter(year_of_first_breast_cancer_diagnosis >= 2020)
  
  message("Flag detected: filtering to participants diagnosed since 2020 (n = ",
          nrow(patient_profile_eligibility), ")")
  
  eligible_ids <- patient_profile_eligibility %>%
    filter(year_of_first_breast_cancer_diagnosis >= 2020) %>%
    pull(patient_id)
  
  # Restrict each survey table to eligible participants only
  manifest <- manifest %>%
    mutate(data = map(data, ~ .x %>%
                        filter(patient_id %in% eligible_ids) %>%
                        distinct(patient_id, .keep_all = TRUE)
    ))
  
} else {
  
  message("No filtering flag detected: using full eligibility file (n = ",
          nrow(patient_profile_eligibility), ")")
  
  # Recode multi-select ethnicity responses and remove duplicate patients
  manifest <- manifest %>%
    mutate(data = map(data, function(df) {
      df %>%
        mutate(across(
          contains("ethnicity", ignore.case = TRUE),
          ~ if_else(str_detect(.x, "\\|"), "other_race_multiracial", .x)
        )) %>%
        distinct(patient_id, .keep_all = TRUE)
    }))
}

setwd("../")

# Combine files belonging to the same survey
combined_by_survey <- manifest %>%
  group_by(survey) %>%
  summarise(data = list(bind_rows(data)), .groups = "drop")

survey_tables <- set_names(combined_by_survey$data, combined_by_survey$survey)

# Recode multi-select ethnicity responses at the combined survey level
survey_tables <- survey_tables %>%
  map(~ .x %>%
        mutate(across(
          contains("ethnicity", ignore.case = TRUE),
          ~ if_else(str_detect(.x, "\\|"), "other_race_multiracial", .x)
        ))
  )

list2env(survey_tables, envir = .GlobalEnv)

# Make tables one row per participant
survey_tables <- survey_tables %>%
  map(~ .x %>%
        group_by(patient_id) %>%
        summarise(
          across(everything(), ~ first(na.omit(.x))),
          .groups = "drop"
        )
  )

about_you <- about_you %>%
  group_by(patient_id) %>%
  summarise(
    across(everything(), ~ first(na.omit(.x))),
    .groups = "drop"
  )

quality_of_life <- quality_of_life %>%
  group_by(patient_id) %>%
  summarise(
    across(everything(), ~ first(na.omit(.x))),
    .groups = "drop"
  )

family_health_history <- family_health_history %>%
  group_by(patient_id) %>%
  summarise(
    across(everything(), ~ first(na.omit(.x))),
    .groups = "drop"
  )

social_determinants_of_health <- social_determinants_of_health %>%
  group_by(patient_id) %>%
  summarise(
    across(everything(), ~ first(na.omit(.x))),
    .groups = "drop"
  )

# Generate and export a summary table for each survey
for (survey_name in names(survey_tables)) {
  
  df <- survey_tables[[survey_name]]
  
  summary_table <- df %>%
    tbl_summary(
      include = -any_of(c("patient_id", "task_id", "task_version", "patient_task_id")),
      missing_text = "Not Applicable"
    ) %>%
    modify_header(label ~ "**Question**") %>%
    modify_caption(paste0("**", survey_name, " Summary**")) %>%
    bold_labels()
  
  summary_table %>%
    as_gt() %>%
    tab_header(title = md(paste0("**", survey_name, " Responses**"))) %>%
    gtsave(paste0(survey_name, "_summary.docx"))
}

# Build a tidy question/response/total/percentage table for every survey, suitable for a Terra
# table upload. Uses the same one-row-per-participant survey_tables and excluded ID columns
# as the docx summary tables above, so the two outputs describe the same underlying counts.
clean_token <- function(x) {
  x <- tolower(as.character(x))
  x <- if_else(is.na(x) | x == "", "not_applicable", x)
  x <- str_replace_all(x, "[^a-z0-9]+", "_")
  str_replace_all(x, "^_+|_+$", "")
}

summarize_variable <- function(df, variable) {
  n_total <- nrow(df)
  df %>%
    mutate(.value = clean_token(.data[[variable]])) %>%
    count(.value, name = "total") %>%
    mutate(
      question = clean_token(variable),
      response = .value,
      percentage = round(total / n_total * 100, 1)
    ) %>%
    select(question, response, total, percentage)
}

excluded_id_columns <- c("patient_id", "task_id", "task_version", "patient_task_id")

questionnaire_summary <- map_dfr(names(survey_tables), function(survey_name) {
  df <- survey_tables[[survey_name]]
  variables <- setdiff(names(df), excluded_id_columns)
  map_dfr(variables, ~ summarize_variable(df, .x)) %>%
    mutate(survey_title = survey_name)
})

write_csv(questionnaire_summary, "questionnaire_summary.csv")

# Make document listing data collection counts (questionnaires completed and MR abstracted)
if (five_year_diagnosis) {
  MR_demographics <- read.csv("Data/demographics.csv")
  MR_demographics <- MR_demographics[MR_demographics$patient_id %in% patient_profile_eligibility$patient_id,]
} else {
  MR_demographics <- read.csv("Data/demographics.csv")
}

fileConn<-file("data_collection_counts.txt")
writeLines(c(paste("ShareForCures Participants: ", nrow(patient_profile_eligibility)),
             paste("Medical Records: ", nrow(MR_demographics)),
             paste("Quality of Life: ", nrow(quality_of_life)),
             paste("Family Health History: ", nrow(family_health_history)),
             paste("Social Determinants of Health: ", nrow(social_determinants_of_health))), fileConn)
close(fileConn)
