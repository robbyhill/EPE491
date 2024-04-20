library(tidyverse)
library(fixest)
library(stringr)
library(modelsummary)
library(haven)
library(glue)
library(gt)
library(scales)
library(car)

# Prepare census population data ------------------------------------------

# Get population data
years <- seq(1940, 1990, by = 10)
file_paths <- paste0("data/census/population_by_race/interpolated/population_tract_adjusted_", years, ".csv")

# Function to dynamically read CSV and select existing necessary columns
read_and_prepare <- function(path, year) {
  # Read the entire CSV to get column names without specifying column types yet
  all_cols <- colnames(read_csv(path, col_types = cols(), n_max = 0))
  
  # Define potential columns of interest
  cols_of_interest <- c("FID_1990", paste0("white_", year), paste0("black_", year), paste0("nonwhite_", year), paste0("other_", year))
  
  # Determine which columns to keep based on their presence
  cols_to_keep <- cols_of_interest[cols_of_interest %in% all_cols]
  
  # Read the CSV again, now selecting only the columns to keep
  data <- read_csv(path, col_types = cols()) |>
    dplyr::select(all_of(cols_to_keep)) |>
    mutate(year = year) # Add a year column for reference
  
  return(data)
}

# Load, prepare, and store datasets in a list
data_list <- map2(file_paths, years, read_and_prepare)

# Join all datasets by FID_1990
pop <- reduce(data_list, full_join, by = "FID_1990") |> 
  dplyr::select(-matches("^year")) |> 
  mutate(across(contains("_"), ~as.numeric(as.character(.)))) |> 
  rename(black_1940 = nonwhite_1940) 

# Pivot the data to a long format and calculate black share for each tract x year
pop <- pop |>
  pivot_longer(
    cols = -FID_1990,
    names_to = c("race", "year"), 
    names_sep = "_", 
    values_to = "population"
  ) |> 
  pivot_wider(
    names_from = race, values_from = population, values_fill = list(population = 0)
  ) |> 
  group_by(year, FID_1990) |> 
  mutate(
    total_pop = sum(white, black, other, na.rm = TRUE), 
    black_share = (black / total_pop) * 100, 
    white_share = (white / total_pop) * 100, 
    black_to_white = (black_share / white_share)
  ) |> 
  ungroup()


# Prepare urban renewal data ----------------------------------------------

# Read urban renewal project .csv files
ur_cov <- read_csv("data/urban_renewal_projects_coverage.csv")
ur_time <- read_csv("data/urban_renewal_projects_time.csv")
tract_town <- read_csv("data/census/tracts_by_town.csv")
reloc <- read_csv("data/housing_relocation.csv")
dest <- read_csv("data/housing_destination.csv")

# Join ur_cov with ur_time
joined_data <- ur_cov |>
  left_join(ur_time, by = "project_id")

# Generate a sequence of years
years_seq <- 1940:1990

# Expand joined_data to include one row per year per tract
expanded_data <- joined_data |> 
  rowwise() |>
  mutate(year = list(years_seq[years_seq >= project_start])) |>
  unnest(year) |>
  dplyr::select(FID_1990, project_id, year, share_of_tract)

# Pivot wider to create columns for each project's percentage impact per year
impact_data_wide <- expanded_data |>
  pivot_wider(
    names_from = project_id,
    values_from = share_of_tract,
    names_prefix = "project_",
    values_fill = list(share_of_tract = 0) # Assuming 0 impact for years before the project started
  )

# Join impact_data_wide with pop
pop$year <- as.integer(pop$year)

df <- pop |>
  left_join(impact_data_wide, by = c("FID_1990", "year")) |>
  mutate(across(everything(), ~replace(., is.na(.), 0))) |> 
  left_join(tract_town, by = "FID_1990")

# Add a new column 'sum_impacted' which is the row-wise sum of all the project impact columns
projects <- df |>
  colnames() |> 
  str_subset("project_")

df$sum_impacted <- rowSums(df[projects], na.rm = TRUE)
df <- df |> 
  mutate(project_binary = ifelse(sum_impacted > 0, 1, 0), 
         impact_share = sum_impacted / 100) 


# Prepare urban renewal project microdata -------------------------------------------------------

# Define the sequence of decennial years
decennial_years <- seq(1940, 1990, by = 10)

# Reshape ur_time to long format for the construction variables
ur_time_long <- ur_time |>
  # Repeat each row for each decennial year
  mutate(year = list(decennial_years)) |>
  unnest(year) |>
  # Retain the construction variables as is if year is greater than or equal to project_start, else set to 0
  mutate(across(federal_funds:other_cost, ~ if_else(year >= project_start, .x, 0))) |>
  # Select only the necessary columns, adjust this according to your dataframe
  dplyr::select(project_id, year, federal_funds:other_cost)

# Now ur_time_long is in the format you want, with one row for each project for each decennial year

# Step 1: Merge ur_time_long with ur_cov
joined_data <- ur_time_long |>
  left_join(ur_cov, by = "project_id")

# Step 2: Calculate the construction statistics for each tract-year
# Multiply the construction variables by the percentage share (share_of_project)
joined_data <- joined_data |>
  mutate(across(federal_funds:other_cost, ~ .x * share_of_project))

# Now, aggregate this data by FID_1990 and year
tract_year_construction_stats <- joined_data |>
  group_by(FID_1990, year) |>
  summarise(across(federal_funds:other_cost, sum, na.rm = TRUE)) |>
  ungroup()

# Step 3: Merge the aggregated data with the main dataframe 'df' by FID_1990 and year
final_df <- df |>
  left_join(tract_year_construction_stats, by = c("FID_1990", "year")) |> 
  mutate(total_units = low_income_units + middle_income_units + high_income_units, 
         total_units_cost = low_income_cost + middle_income_cost + high_income_cost, 
         non_housing_costs = commercial_cost + other_cost, 
         non_development_costs = commercial_cost + other_cost + rehab_cost, 
         redev_costs = federal_funds + local_funds + cash_funds + credit_funds)

# Subset data for New Haven town and New Haven county
df_nhv <- final_df |> 
  filter(town == "New Haven") 

df_county <- final_df |> 
  filter(total_pop > 0)


# Urban Renewal Projects Summary Statistics -------------------------------

ur_time_summary <- ur_time |> 
  mutate(
    total_units = high_income_units + middle_income_units + low_income_units, 
    total_units_cost = high_income_cost + middle_income_cost + low_income_cost, 
    area = area * 0.000247105, 
    clearance_share = clearance_share * 100
  ) |> 
  pivot_longer(
    cols = c(plan_start, project_start, project_end, area, federal_funds, local_funds, cash_funds, credit_funds, clearance_share,
             total_units, total_units_cost, rehab_units, rehab_cost,
             commercial_cost, other_cost),
    names_to = "variable",
    values_to = "value"
  ) |>
  group_by(variable) |> 
  summarize(
    mean = mean(value, na.rm = TRUE), 
    sd = sd(value, na.rm = TRUE), 
    n = sum(!is.na(value))
  ) |> 
  mutate(
    category = dplyr::recode(
      variable,
      plan_start = "Timeline", 
      project_start = "Timeline", 
      project_end = "Timeline", 
      area = "Project Characteristics", 
      clearance_share = "Project Characteristics", 
      federal_funds = "Funding (M 1967 USD)", 
      local_funds = "Funding (M 1967 USD)", 
      cash_funds = "Funding (M 1967 USD)", 
      credit_funds = "Funding (M 1967 USD)", 
      total_units = "Housing Development", 
      total_units_cost = "Housing Development", 
      rehab_units = "Housing Development", 
      rehab_cost = "Housing Development", 
      .default = "Non-Residential Development (M 1967 USD)"
    )) |> 
  mutate(variable = recode_factor(
    variable, 
    plan_start = "Planning Start Year", 
    project_start = "Project Start Year", 
    project_end = "Project End Year", 
    area = "Area (acres)", 
    clearance_share = "Percentage Cleared", 
    federal_funds = "Federal Funds", 
    local_funds = "City & State Funds", 
    cash_funds = "City Cash", 
    credit_funds = "Non-Cash Credits", 
    total_units = "New Units", 
    total_units_cost = "Spending on New Units (M 1967 USD)", 
    rehab_units = "Units Rehabilitated", 
    rehab_cost = "Spending on Rehabilitation (M 1967 USD)", 
    .default = "Non-Residential Development", 
    other_cost = "Public Spending", 
    commercial_cost = "Private Spending"
  )) |> 
  arrange(variable) |>
  group_by(category) |> 
  gt() 

ur_time_summary <- ur_time_summary |>
  cols_align("left", columns = variable) |> 
  cols_align("center", columns = c(mean, sd, n)) |> 
  gt::cols_label(
    variable = "",
    mean = "Mean", 
    sd = "Std. Dev.", 
    n = "N"
  ) |> 
  fmt(
    columns = vars(mean, sd),
    rows = category == "Timeline",
    fns = function(x) sprintf("%.0f", x)  # No decimal places for years
  ) |> 
  fmt(
    columns = vars(mean, sd),
    rows = category %in% c("Project Characteristics", "Funding (M 1967 USD)", "Non-Residential Development (M 1967 USD)"),
    fns = function(x) sprintf("%.2f", x)  
  ) |> 
  fmt(
    columns = vars(mean, sd),
    rows = variable %in% c("New Units", "Units Rehabilitated"),
    fns = function(x) sprintf("%.0f", x)  # No decimal places for unit counts
  ) |>
  fmt(
    columns = vars(mean, sd),
    rows = variable %in% c("Spending on New Units (M 1967 USD)", "Spending on Rehabilitation (M 1967 USD)"),
    fns = function(x)
      sprintf("%.2f", x)  # No decimal places for unit counts
  ) |> 
  tab_style(style = cell_text(weight = "bold"), 
            locations = cells_row_groups()) |> 
  tab_style(style = cell_text(weight = "bold"), 
            locations = cells_column_labels())

ur_time_summary |>  gtsave("figures/project_summary.tex")


# Main results ---------------------------------------------------------
# Run main regression of black share on urban renewal project coverage
reg_form <- glue("black_share ~ sum_impacted | FID_1990 + year")
regs <- list(reg1 <- fixest::feols(as.formula(reg_form), df_nhv),
             reg2 <- feols(as.formula(reg_form), df_county))

# Visualize regression results
table_main <- modelsummary(regs, 
             gof_map = c("nobs"),
             coef_map = c(
               "sum_impacted" = "Project coverage"), 
             add_rows = data.frame(
               label = c("Tracts", "Tract FE", "Year FE"),
               reg1 = c("29", "Yes", "Yes"),
               reg2 = c("184", "Yes", "Yes")), 
             output = "gt"
)

table_main |> gtsave("figures/table_main.tex")


# Visualizations ----------------------------------------------------------

# Calculate index of dissimilarity
dissimilarity_index <- df_nhv |>
  group_by(year) |>
  # Calculate the total population of black and white for each year
  summarize(total_black = sum(black), total_white = sum(white)) |>
  # Calculate the dissimilarity index by year
  left_join(df_nhv, by = "year") |>
  mutate(dissim_index = 0.5 * abs(black / total_black - white / total_white)) |>
  # Sum the dissimilarity index for each year
  group_by(year) |>
  summarize(dissimilarity = sum(dissim_index))

di_plot <- dissimilarity_index |> 
  ggplot(aes(year, dissimilarity)) +
  geom_point() + 
  theme_bw() +  
  scale_y_continuous(limits = c(0, 1)) + 
  annotate("rect", xmin = 1957, xmax = 1974, ymin = -Inf, ymax = Inf, alpha = 0.4, fill = "gray") + 
  labs(x = "", y = "Dissimilarity Index")

ggsave("figures/di_plot.png", width = 6, height = 4, dpi = 300)

# Relocation and Destination ----------------------------------------------
relocation_data <- reloc |>
  filter(!is.na(project_id)) |> 
  group_by(fam_or_ind) |>
  summarise(
    White = sum(est_relocated_w, na.rm = TRUE),
    Nonwhite = sum(est_relocated_nw, na.rm = TRUE)
  ) |>
  pivot_longer(cols = c(White, Nonwhite), names_to = "race", values_to = "relocations")

# Update the factor levels to ensure the correct order
relocation_data$race <- factor(relocation_data$race, levels = c("White", "Nonwhite"))
relocation_data$fam_or_ind <- factor(relocation_data$fam_or_ind, levels = c("fam", "ind"))

# Plot using ggplot2 with default bar widths and dodging
ggplot(relocation_data, aes(x = fam_or_ind, y = relocations, fill = race)) +
  geom_bar(stat = "identity", position = position_dodge()) +  # Default dodge width
  scale_fill_manual(values = c("White" = "#ff7f7f", "Nonwhite" = "#73b2ff")) +
  labs(title = "",
       x = "",
       y = "Total Relocations",
       fill = "Race") +
  scale_x_discrete(labels = c("fam" = "Families", "ind" = "Individuals")) +
  theme_minimal() +
  theme(legend.position = "right") + 
  scale_y_continuous(labels = comma)  # Format the y-axis labels to include commas

ggsave("figures/relocation_graph.png", width = 6, height = 4, dpi = 300)

summarize_housing_type <- function(data, white_col, nonwhite_col, housing_type_name) {
  data |>
    filter(!is.na(project_id)) |> 
    group_by(fam_or_ind) |>
    summarise(
      White = sum(get(white_col), na.rm = TRUE),
      Nonwhite = sum(get(nonwhite_col), na.rm = TRUE)
    ) |>
    pivot_longer(cols = c(White, Nonwhite), names_to = "race", values_to = "relocations") |>
    mutate(housing_type = housing_type_name)
}

# Apply the function to each housing type
public_data <- summarize_housing_type(dest, "to_public_w", "to_public_nw", "Public Housing")
private_data <- summarize_housing_type(dest, "to_private_w", "to_private_nw", "Private Rents")
purchase_data <- summarize_housing_type(dest, "to_purchase_w", "to_purchase_nw", "Ownership")
left_nhv_data <- summarize_housing_type(dest, "left_nhv_w", "left_nhv_nw", "Left New Haven")

# Combine all the data
all_housing_data <- bind_rows(public_data, private_data, purchase_data, left_nhv_data)

# Update the factor levels to ensure the correct order
all_housing_data$race <- factor(all_housing_data$race, levels = c("White", "Nonwhite"))
all_housing_data$fam_or_ind <- factor(all_housing_data$fam_or_ind, levels = c("fam", "ind"))
all_housing_data$housing_type <- factor(all_housing_data$housing_type, levels = c("Public Housing", "Private Rents", "Ownership", "Left New Haven"))

# Plot using ggplot2 with facet_wrap for each housing type
ggplot(all_housing_data, aes(x = fam_or_ind, y = relocations, fill = race)) +
  geom_bar(stat = "identity", position = position_dodge()) +
  scale_fill_manual(values = c("White" = "#ff7f7f", "Nonwhite" = "#73b2ff")) +
  labs(y = "Total Destinations",
       x = "", 
       fill = "Race") +
  scale_x_discrete(labels = c("fam" = "Families", "ind" = "Individuals")) +
  theme_minimal() +
  theme(legend.position = "bottom") +
  facet_wrap(~housing_type) +
  scale_y_continuous(labels = comma)

ggsave("figures/destination_graph.png", width = 6, height = 4, dpi = 300)


# Total Population of New Haven by Race -----------------------------------

# Assuming df_nhv is your original data frame
pops_by_race <- df_nhv |> 
  summarize(total_white = sum(white), 
            total_black = sum(black),
            .by = year) |> 
  pivot_longer(cols = c(total_white, total_black),  # specify columns directly
               names_to = "population_type", 
               values_to = "population") 

# Recode the population_type within the tibble
# Ensure that population_type is a character if you're using recode()
pops_by_race$population_type <-
  as.character(pops_by_race$population_type)
pops_by_race$population_type <- pops_by_race |>
  pull(population_type) |>
  dplyr::recode("total_white" = "White Population",
                "total_black" = "Black Population") |>
  factor(levels = c("White Population", "Black Population"))


# Now plot using the modified tibble
ggplot(pops_by_race, aes(x = year, y = population, color = population_type)) + 
  geom_line() + 
  labs(x = "Year",
       y = "Population",
       color = "Population Type") +
  scale_y_continuous(labels = scales::comma) + # Format y-axis labels with commas
  scale_color_manual(values = c("White Population" = "#ff7f7f", "Black Population" = "#73b2ff")) + # Assign colors
  theme_minimal() 

ggsave("figures/pop_over_time.png", width = 6, height = 4, dpi = 300)


# Robustness: black share on housing type ---------------------------------

housing_regs <- list(
  units <- feols(black_share ~ total_units | FID_1990 + year, df_nhv),
  units3 <- feols(black_share ~ low_income_units + middle_income_units + high_income_units | FID_1990 + year, df_nhv), 
  costs <- feols(black_share ~ total_units_cost | FID_1990 + year, df_nhv),
  costs3 <- feols(black_share ~ low_income_cost + middle_income_cost + high_income_cost | FID_1990 + year, df_nhv)
)

table_housing <- modelsummary(
  housing_regs,
  gof_map = c("nobs"),
  coef_map = c(
    "total_units" = "Total",
    "low_income_units" = "Low-income public",
    "middle_income_units" = "Middle-income public",
    "high_income_units" = "High-income private",
    "total_units_cost" = "Total",
    "low_income_cost" = "Low-income public",
    "middle_income_cost" = "Middle-income public",
    "high_income_cost" = "High-income private"
  ),
  add_rows = data.frame(
    label = c("Tracts", "Tract FE", "Year FE"),
    units = c("22", "Yes", "Yes"),
    units3 = c("22", "Yes", "Yes"), 
    costs = c("22", "Yes", "Yes"),
    costs3 = c("22", "Yes", "Yes")
  ),
  output = "gt"
)

table_housing <- table_housing |> 
  cols_width(everything() ~ px(125)) |>
  tab_spanner(
    label = "Units", 
    columns = 2:3
  ) |> 
  tab_spanner(
    label = "Spending", 
    columns = 4:5
  ) 

table_housing |> gtsave("figures/table_housing.tex")


# Robustness: black share on corporate and other costs --------------------

non_dev <- feols(black_share ~ non_housing_costs | FID_1990 + year, df_nhv)
non_dev3 <- feols(black_share ~ commercial_cost + other_cost | FID_1990 + year, df_nhv)

spending_regs <- list(non_dev, non_dev3)

test_add_rows <- data.frame(
  label = c("Tracts", "Tract FE", "Year FE"),
  non_dev = c("22", "Yes", "Yes"),   # NA for a missing value in the extra row
  non_dev3 = c("22", "Yes", "Yes")   # NA for a missing value in the extra row
)

table_spending <- modelsummary(spending_regs, 
             gof_map = c("nobs"),
             coef_map = c(
               "non_housing_costs" = "Total non-residential spending",
               "other_cost" = "Public spending", 
               "commercial_cost" = "Private spending"
             ),
             add_rows = test_add_rows,
             output = "gt"
)

table_spending |> gtsave("figures/table_spending.tex")
