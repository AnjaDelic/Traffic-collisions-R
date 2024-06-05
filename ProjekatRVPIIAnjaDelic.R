
#Initial setup
#Incijalna podešavanja, instaliranje i učitavanje paketa

Sys.setenv(JAVA_HOME = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home")


#Instalacija neophodnih paketa
# install.packages("sparklyr")
# install.packages("dplyr")
# install.packages("tidyr")
# install.packages("ggplot2")
# install.packages("knitr")
# install.packages("leaflet")
# install.packages("leaflet.extras")
# install.packages("glmnet")
# install.packages("caret")
# install.packages("e1071")
# install.packages("pROC")
# install.packages("ROCR")
# install.packages("reshape2")
#Include required packages

library(sparklyr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
library(leaflet)
library(leaflet.extras)
library(caret)
library(e1071)
library(pROC)
library(ROCR)
library(reshape2)
library(glmnet)
library(stringr)


#Install Spark

#Podešavanje spark sesije
spark_install("3.0")

knitr::opts_knit$set(root.dir = "/mnt/StorageSpace/StorageSpace/repositories/ghi-predicting")



conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "16G"
conf$spark.memory.fraction <- 0.9


# Connect to Spark with the updated configuration
sc <- spark_connect(master = "local",version = "3.0", config = conf)


#Definisanje putanje do skupa podataka i učitavanje skupa podataka
file_path <- "Downloads/Motor_Vehicle_Collisions_-_Crashes.csv"

#Read dataset
collisions.basic <- spark_read_csv(sc, 
                                   name = "Motor_Vehicle_Collisions_Crashes", 
                                   path = file_path, 
                                   header = TRUE, 
                                   memory = TRUE)

# Display the first 15 rows

# #Prikaz prvih 15 uzoraka iz skupa podataka
# head_data <- collisions.basic %>% head(15) %>% collect()
# print(head_data)
# 
# # Get the number of rows
# 
# #Prikaz broja uzoraka (redova) i obeležja (kolona) skupa podataka
# num_rows <- sdf_nrow(collisions.basic)
# print(paste("Number of samples:", num_rows))
# 
# # Get the number of columns
# num_cols <- length(sdf_schema(collisions.basic))
# print(paste("Number of features:", num_cols))
# 
# # Check for duplicated columns (features)
# 
# #Provera postojanja duplih vrednosti obeležja i uzoraka
# column_names <- colnames(collisions.basic)
# duplicated_columns <- column_names[duplicated(column_names)]
# if (length(duplicated_columns) > 0) {
#   print("Duplicated columns found:")
#   print(duplicated_columns)
# } else {
#   print("No duplicated columns found.")
# }
# 
# # Check for duplicated rows
# duplicated_rows_count <- collisions.basic %>%
#   group_by_all() %>%
#   filter(n() > 1) %>%
#   summarise(count = n(), .groups = 'drop') %>%
#   collect() %>%
#   nrow()
# 
# if (duplicated_rows_count > 0) {
#   print(paste("Number of duplicated rows:", duplicated_rows_count))
# } else {
#   print("No duplicated rows found.")
# }

#######################################
#Dealing with NA values

# Function to calculate percentage of NA values in each column

#Obrada nedostajućih vrednosti 
calculate_na_percentage <- function(df) {
  num_rows <- sdf_nrow(df)
  
  na_percentages <- df %>%
    # ~ must be used to introduce annonymous function inside summarise_all function
    summarise_all(~(sum(ifelse(is.na(.), 1, 0)) / num_rows) * 100) %>%
    collect() %>%
    #converts to longer format for better readability
    pivot_longer(cols = everything(), names_to = "Column", values_to = "NA_Percentage")
  
  return(na_percentages)
}

# Calculate and print the percentage of NA values for each column
na_percentages <- calculate_na_percentage(collisions.basic)
print("Percentage of NaN values in each column:")
print(n=29,na_percentages)

# Drop rows with NA values in COLLISION_ID
collisions <- collisions.basic %>%
  filter(!is.na(COLLISION_ID))
# Replace NaN with No vehicle for contributing factor
collisions <- collisions.basic %>%
  mutate(
    CONTRIBUTING_FACTOR_VEHICLE_2 = ifelse(is.na(CONTRIBUTING_FACTOR_VEHICLE_2), "Unspecified", CONTRIBUTING_FACTOR_VEHICLE_2),
    CONTRIBUTING_FACTOR_VEHICLE_3 = ifelse(is.na(CONTRIBUTING_FACTOR_VEHICLE_3), "Unspecified", CONTRIBUTING_FACTOR_VEHICLE_3),
    CONTRIBUTING_FACTOR_VEHICLE_4 = ifelse(is.na(CONTRIBUTING_FACTOR_VEHICLE_4), "Unspecified", CONTRIBUTING_FACTOR_VEHICLE_4),
    CONTRIBUTING_FACTOR_VEHICLE_5 = ifelse(is.na(CONTRIBUTING_FACTOR_VEHICLE_5), "Unspecified", CONTRIBUTING_FACTOR_VEHICLE_5)
  )
  # Replace corresponding vehicle type features with 0
  collisions <- collisions %>%
    mutate(
      VEHICLE_TYPE_CODE_2 = ifelse(is.na(VEHICLE_TYPE_CODE_2) , "UNKNOWN", VEHICLE_TYPE_CODE_2),
      VEHICLE_TYPE_CODE_3 = ifelse(is.na(VEHICLE_TYPE_CODE_3), "UNKNOWN", VEHICLE_TYPE_CODE_3),
      VEHICLE_TYPE_CODE_4 = ifelse(is.na(VEHICLE_TYPE_CODE_4) , "UNKNOWN", VEHICLE_TYPE_CODE_4),
      VEHICLE_TYPE_CODE_5 = ifelse(is.na(VEHICLE_TYPE_CODE_5) , "UNKNOWN", VEHICLE_TYPE_CODE_5)
    )
  
  #Replace cross and off street
  collisions <- collisions %>%
    mutate(
      CROSS_STREET_NAME = ifelse(is.na(CROSS_STREET_NAME), "Unknown", CROSS_STREET_NAME),
      OFF_STREET_NAME = ifelse(is.na(OFF_STREET_NAME) || OFF_STREET_NAME=="", "Not provided", OFF_STREET_NAME)
    )
  
  # Drop rows with NA values in COLLISION_ID
  collisions <- collisions %>%
    filter(!is.na(CONTRIBUTING_FACTOR_VEHICLE_1) &
             !is.na(LATITUDE) &
             !is.na(LONGITUDE) &
             !is.na(BOROUGH) &
             !is.na(ZIP_CODE) &
             !is.na(ON_STREET_NAME) &
             !is.na(VEHICLE_TYPE_CODE_1) &
             !is.na(VEHICLE_TYPE_CODE_2) &
             !is.na(VEHICLE_TYPE_CODE_3) &
             !is.na(VEHICLE_TYPE_CODE_4) &
             !is.na(VEHICLE_TYPE_CODE_5) &
             !is.na(NUMBER_OF_PERSONS_INJURED) &
             !is.na(NUMBER_OF_PERSONS_KILLED) )
  


  # Get the number of rows
  num_rows <- sdf_nrow(collisions)
  print(paste("Number of samples:", num_rows))


  # #######################################
  #Augmenting dataset by duplicating it 3 times

  # # #Povećavanje skupa podataka
  for(i in 1:2){
    
    # Replicate each row by unioning the DataFrame with itself
    collisions <- sdf_bind_rows(collisions, collisions)

    # Verify the duplication
    num_rows_after_duplication <- sdf_nrow(collisions)
    print(paste("Number of samples after duplication:", num_rows_after_duplication))

  }


  #######################################
  # #Data insight
  # 
  # # Frequency table for categorical columns
  # categorical_cols <- c("BOROUGH", "CONTRIBUTING_FACTOR_VEHICLE_1", "CONTRIBUTING_FACTOR_VEHICLE_2", "CONTRIBUTING_FACTOR_VEHICLE_3","CONTRIBUTING_FACTOR_VEHICLE_4","CONTRIBUTING_FACTOR_VEHICLE_5",
  #                       "VEHICLE_TYPE_CODE_1","VEHICLE_TYPE_CODE_2","VEHICLE_TYPE_CODE_3","VEHICLE_TYPE_CODE_4","VEHICLE_TYPE_CODE_5",
  #                       "ON_STREET_NAME","CROSS_STREET_NAME","OFF_STREET_NAME")
  # frequency_table <- vector("list", length(categorical_cols))
  # 
  # for (i in seq_along(categorical_cols)) {
  #   col <- categorical_cols[i]
  #   freq_table <- collisions %>%
  #     group_by(!!sym(col)) %>%
  #     summarise(count = n()) %>%
  #     collect() %>%
  #     arrange(desc(count))
  #   frequency_table[[i]] <- freq_table
  # }
  # 
  # names(frequency_table) <- categorical_cols
  # 
  # print("Frequency Tables:")
  # print(frequency_table)
  # 

  # Izračunavanje procenta vrednosti "Unspecified" za kolone "CONTRIBUTING_FACTOR_VEHICLE_1","CONTRIBUTING_FACTOR_VEHICLE_2",.."CONTRIBUTING_FACTOR_VEHICLE_5"
  unspecified_percentage <- collisions %>%
    filter(CONTRIBUTING_FACTOR_VEHICLE_1 == "Unspecified") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Procentualni udeo vrednosti 'Unspecified' za kolonu 'CONTRIBUTING_FACTOR_VEHICLE_2':", unspecified_percentage, "%"))
  
  unspecified_percentage <- collisions %>%
    filter(CONTRIBUTING_FACTOR_VEHICLE_2 == "Unspecified") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Procentualni udeo vrednosti 'Unspecified' za kolonu 'CONTRIBUTING_FACTOR_VEHICLE_3':", unspecified_percentage, "%"))
  
  unspecified_percentage <- collisions %>%
    filter(CONTRIBUTING_FACTOR_VEHICLE_3 == "Unspecified") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Procentualni udeo vrednosti 'Unspecified' za kolonu 'CONTRIBUTING_FACTOR_VEHICLE_4':", unspecified_percentage, "%"))
  
  unspecified_percentage <- collisions %>%
    filter(CONTRIBUTING_FACTOR_VEHICLE_4 == "Unspecified") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Procentualni udeo vrednosti 'Unspecified' za kolonu 'CONTRIBUTING_FACTOR_VEHICLE_4':", unspecified_percentage, "%"))
  
  unspecified_percentage <- collisions %>%
    filter(CONTRIBUTING_FACTOR_VEHICLE_5 == "Unspecified") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Procentualni udeo vrednosti 'Unspecified' za kolonu 'CONTRIBUTING_FACTOR_VEHICLE_5':", unspecified_percentage, "%"))
  
  # S obzirom na veliko prisustvo 'Unspecified' vrednosti odabrane kolone ne doprinose novim informacijama o sudarima, te se mogu izbaciti.
  cols_to_drop <- c("CONTRIBUTING_FACTOR_VEHICLE_5","CONTRIBUTING_FACTOR_VEHICLE_4","CONTRIBUTING_FACTOR_VEHICLE_3","CONTRIBUTING_FACTOR_VEHICLE_2" )
  collisions <- collisions %>% 
    select(-any_of(cols_to_drop))
  
  # Izračunavanje procenta vrednosti  "UNKNOWN" za kolone "VEHICLE_TYPE_CODE_1",.."VEHICLE_TYPE_CODE_5"
  unknown_percentage_1 <- collisions %>%
    filter(VEHICLE_TYPE_CODE_1 == "UNKNOWN") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Percentage of 'UNKNOWN' value for column 'VEHICLE_TYPE_CODE_1':", unknown_percentage_1, "%"))
  
  unknown_percentage_2 <- collisions %>%
    filter(VEHICLE_TYPE_CODE_2 == "UNKNOWN") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Percentage of 'UNKNOWN' value for column 'VEHICLE_TYPE_CODE_2':", unknown_percentage_2, "%"))
  
  unknown_percentage_3 <- collisions %>%
    filter(VEHICLE_TYPE_CODE_3 == "UNKNOWN") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Percentage of 'UNKNOWN' value for column 'VEHICLE_TYPE_CODE_3':", unknown_percentage_3, "%"))
  
  unknown_percentage_4 <- collisions %>%
    filter(VEHICLE_TYPE_CODE_4 == "UNKNOWN") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Percentage of 'UNKNOWN' value for column 'VEHICLE_TYPE_CODE_4':", unknown_percentage_4, "%"))
  
  unknown_percentage_5 <- collisions %>%
    filter(VEHICLE_TYPE_CODE_5 == "UNKNOWN") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Percentage of 'UNKNOWN' value for column 'VEHICLE_TYPE_CODE_5':", unknown_percentage_5, "%"))
  
  # S obzirom na veliko prisustvo 'UNKNOWN' vrednosti odabrane kolone ne doprinose novim informacijama o sudarima, te se mogu izbaciti.
  cols_to_drop <- c("VEHICLE_TYPE_CODE_5","VEHICLE_TYPE_CODE_4","VEHICLE_TYPE_CODE_3","VEHICLE_TYPE_CODE_2" )
  collisions <- collisions %>% 
    select(-any_of(cols_to_drop))
  
  # Izračunavanje procenta vrednosti "Not provided" ili "" za kolonu "OFF_STREET_NAME"
  unspecified_percentage <- collisions %>%
    filter(OFF_STREET_NAME == "Not provided"|| OFF_STREET_NAME == "") %>%
    sdf_nrow() / sdf_nrow(collisions) * 100
  print(paste("Procentualni udeo vrednosti 'Unspecified' za kolonu 'OFF_STREET_NAME':", unspecified_percentage, "%"))
  
  # S obzirom na veliko prisustvo 'Not provided' ili praznih vrednosti kolona 'OFF_STREET_NAME' ne doprinosi novim informacijama o sudarima, te se može izbaciti.
  collisions <- collisions %>% 
    select(-OFF_STREET_NAME)
  
  # # Convert numeric columns to integers
  convert_to_integer <- function(df, cols) {
    for (col in cols) {
      # Convert column values to numeric and then to integer
      df <- df %>%
        mutate(!!sym(col) := as.integer(!!sym(col)))
    }
    return(df)
  }

  
  # Takođe primećeno je da je potrebno očistiti kolonu 'VEHICLE_TYPE_CODE_1'.
  # Čišćenje kolone VEHICLE_TYPE_CODE_1
  collisions <- collisions %>%
    mutate(CLEANED_VEHICLE_TYPE = case_when(
      grepl("car|sedan|convertible", tolower(VEHICLE_TYPE_CODE_1)) ~ "Car",
      grepl("truck|tow|tractor|flatbed|dump", tolower(VEHICLE_TYPE_CODE_1)) ~ "Truck",
      grepl("ambulance|ems|fdny", tolower(VEHICLE_TYPE_CODE_1)) ~ "Emergency Vehicle",
      grepl("bike|scooter|e[-\\s]?bike", tolower(VEHICLE_TYPE_CODE_1)) ~ "Bicycle/Scooter",
      grepl("bus", tolower(VEHICLE_TYPE_CODE_1)) ~ "Bus",
      TRUE ~ "Other"
    ))
  
  collisions <- collisions %>%
    mutate(CONTRIBUTING_FACTOR_VEHICLE_1 = 
             case_when(
               CONTRIBUTING_FACTOR_VEHICLE_1 == "Cell Phone (hand-held)" | CONTRIBUTING_FACTOR_VEHICLE_1 == "Cell Phone (hand-Held)" ~ "Cell Phone",
               CONTRIBUTING_FACTOR_VEHICLE_1 == "Drugs (illegal)" | CONTRIBUTING_FACTOR_VEHICLE_1 == "Drugs (Illegal)" ~ "Drugs (Illegal)",
               CONTRIBUTING_FACTOR_VEHICLE_1 == "Illnes" ~ "Illness",
               # Add more cleaning rules as needed
               TRUE ~ CONTRIBUTING_FACTOR_VEHICLE_1  # Keep unchanged if no match
             ))
  
  
  print(collisions)
  
  
  # Columns of interest
  to_numeric_cols <- c("NUMBER_OF_PERSONS_INJURED",
                    "NUMBER_OF_CYCLIST_KILLED",
                    "NUMBER_OF_MOTORIST_INJURED")

  # Convert the columns to integer
  collisions <- convert_to_integer(collisions, to_numeric_cols)

  # 
  # # Define the columns of interest
  # cols_of_interest <- c("LATITUDE", "LONGITUDE", "NUMBER_OF_PERSONS_INJURED",
  #                       "NUMBER_OF_PERSONS_KILLED", "NUMBER_OF_PEDESTRIANS_INJURED",
  #                       "NUMBER_OF_PEDESTRIANS_KILLED", "NUMBER_OF_CYCLIST_INJURED",
  #                       "NUMBER_OF_CYCLIST_KILLED", "NUMBER_OF_MOTORIST_INJURED",
  #                       "NUMBER_OF_MOTORIST_KILLED")
  # 
  # # Loop through each column
  # for (col in cols_of_interest) {
  #   # Extract column values
  # column_values <- collisions %>% pull(col)
  # 
  # # Check if the column is numeric
  # if (is.numeric(column_values)) {
  #   # Compute summary statistics
  #   count <- length(column_values)
  #   min_val <- min(column_values, na.rm = TRUE)
  #   quantile_25 <- quantile(column_values, probs = 0.25, na.rm = TRUE)
  #   median_val <- median(column_values, na.rm = TRUE)
  #   mean_val <- mean(column_values, na.rm = TRUE)
  #   std_val <- sd(column_values, na.rm = TRUE)
  #   quantile_75 <- quantile(column_values, probs = 0.75, na.rm = TRUE)
  #   max_val <- max(column_values, na.rm = TRUE)
  # 
  #   # Print the summary statistics
  #   cat("Summary statistics for column:", col, "\n")
  #   cat("count: ", count, "\n")
  #   cat("mean: ", mean_val, "\n")
  #   cat("std: ", std_val, "\n")
  #   cat("min: ", min_val, "\n")
  #   cat("25%: ", quantile_25, "\n")
  #   cat("50%: ", median_val, "\n")
  #   cat("75%: ", quantile_75, "\n")
  #   cat("max: ", max_val, "\n\n")
  # } else {
  #   cat("Column", col, "contains non-numeric values. Skipping...\n\n")
  # }
  # }
  #######################################
  #Visualisation for one feature
 
  # # Aggregate the count of incidents by borough
  # incidents_by_borough <- collisions %>%
  #   group_by(BOROUGH) %>%
  #   summarise(total_incidents = n()) %>%
  #   collect()
  # 
  # # Calculate percentage of incidents in each borough
  # incidents_by_borough$percentage <- incidents_by_borough$total_incidents / sum(incidents_by_borough$total_incidents) * 100
  # 
  # # Create a pie chart with percentage labels
  # ggplot(incidents_by_borough, aes(x = "", y = percentage, fill = BOROUGH)) +
  #   geom_bar(stat = "identity", width = 1) +
  #   coord_polar("y", start = 0) +
  #   labs(title = "Distribution of Incidents by Borough",
  #        fill = "Borough",
  #        x = NULL,
  #        y = NULL) +
  #   geom_text(aes(label = paste0(round(percentage), "%")), position = position_stack(vjust = 0.5)) +
  #   theme_void() +
  #   theme(legend.position = "right")
  
  # top_contributing_factors <- collisions %>%
  #   group_by(CONTRIBUTING_FACTOR_VEHICLE_1) %>%
  #   summarise(total_crashes = n()) %>%
  #   arrange(desc(total_crashes)) %>%
  #   head(10)
  # 
  # # Create a bar plot
  # ggplot(top_contributing_factors, aes(x = reorder(CONTRIBUTING_FACTOR_VEHICLE_1, -total_crashes), y = total_crashes)) +
  #   geom_bar(stat = "identity", fill = "skyblue") +
  #   labs(x = "Contributing Factors", y = "Total Crashes", title = "Top Contributing Factors to Crashes") +
  #   scale_y_continuous(labels = scales::number_format()) +  # Display y-axis labels as normal numbers
  #   theme_minimal() +
  #   theme(axis.text.x = element_text(angle = 45, hjust = 1))  # Rotate x-axis labels for better readability
  # 
  
  # top_vehicle_type <- collisions %>%
  #   group_by(VEHICLE_TYPE_CODE_1) %>%
  #   summarise(total_crashes = n()) %>%
  #   arrange(desc(total_crashes)) %>%
  #   head(10)
  # 
  # # Create a bar plot
  # ggplot(top_vehicle_type, aes(x = reorder(VEHICLE_TYPE_CODE_1, -total_crashes), y = total_crashes)) +
  #   geom_bar(stat = "identity", fill = "skyblue") +
  #   labs(x = "Vehicle type", y = "Total Crashes", title = "Top Vehicle Types to Crashes") +
  #   scale_y_continuous(labels = scales::number_format()) +  # Display y-axis labels as normal numbers
  #   theme_minimal() +
  #   theme(axis.text.x = element_text(angle = 45, hjust = 1))  # Rotate x-axis labels for better readability

  ###############################################################
  # #Time based analysis
  # 
  # #Number of accidents per year
  # # Extract year from CRASH_DATE
  collisions <- collisions %>%
    mutate(CRASH_YEAR = year(to_date(CRASH_DATE, "MM/dd/yyyy")))

  # # Count number of accidents per year
  # accidents_per_year <- collisions %>%
  #   group_by(CRASH_YEAR) %>%
  #   summarise(total_accidents = n()) %>%
  #   collect()
  # 
  # # Find the year with the highest count
  # max_year <- accidents_per_year$CRASH_YEAR[which.max(accidents_per_year$total_accidents)]
  # 
  # # Plot number of accidents per year with sorted year labels on x-axis
  # ggplot(accidents_per_year, aes(x = factor(CRASH_YEAR), y = total_accidents, fill = CRASH_YEAR == max_year)) +
  #   geom_bar(stat = "identity") +
  #   scale_fill_manual(values = c("navyblue", "red"), guide = FALSE) +  # Manually set fill colors
  #   labs(title = "Number of Accidents Per Year",
  #        x = "Year",
  #        y = "Total Accidents") +
  #   scale_y_continuous(labels = scales::comma) +  # Format y-axis labels as integers
  #   theme_minimal()
  # 
  # 
  # #Number of accidents per month
  # # Extract month from CRASH_DATE
  collisions <- collisions %>%
    mutate(CRASH_MONTH = month(to_date(CRASH_DATE, "MM/dd/yyyy")))

  # # Count number of accidents per month
  # accidents_per_month <- collisions %>%
  #   group_by(CRASH_MONTH) %>%
  #   summarise(total_accidents = n()) %>%
  #   collect()
  # 
  # # Find the month with the highest count
  # max_month <- accidents_per_month$CRASH_MONTH[which.max(accidents_per_month$total_accidents)]
  # 
  # # Plot number of accidents per month with sorted month labels on x-axis
  # ggplot(accidents_per_month, aes(x = factor(CRASH_MONTH), y = total_accidents, fill = CRASH_MONTH == max_month)) +
  #   geom_bar(stat = "identity") +
  #   scale_fill_manual(values = c("navyblue", "red"), guide = FALSE) +  # Manually set fill colors
  #   labs(title = "Number of Accidents Per Month",
  #        x = "Month",
  #        y = "Total Accidents") +
  #   scale_y_continuous(labels = scales::comma) +  # Format y-axis labels as integers
  #   scale_x_discrete(labels = month.abb) +  # Specify month labels
  #   theme_minimal()

  # 
  # #Number of accidents per hour
  # # Extract hour from CRASH_TIME and pad single-digit hours
  collisions <- collisions %>%
    mutate(CRASH_HOUR = lpad(hour(to_timestamp(CRASH_TIME, "H:mm")), 2, "0"))

  # # Count number of accidents per hour
  # accidents_per_hour <- collisions %>%
  #   group_by(CRASH_HOUR) %>%
  #   summarise(total_accidents = n()) %>%
  #   collect()
  # 
  # # Find the hour with the maximum number of accidents
  # max_hour <- accidents_per_hour$CRASH_HOUR[which.max(accidents_per_hour$total_accidents)]
  # 
  # # Plot number of accidents per hour with sorted hour labels on x-axis
  # ggplot(accidents_per_hour, aes(x = factor(CRASH_HOUR, levels = sprintf("%02d", 0:23)), y = total_accidents)) +
  #   geom_bar(stat = "identity", aes(fill = CRASH_HOUR == max_hour)) +
  #   scale_fill_manual(values = c("TRUE" = "red", "FALSE" = "navyblue"), guide = FALSE) +
  #   labs(title = "Number of Accidents Per Hour",
  #        x = "Hour",
  #        y = "Total Accidents") +
  #   scale_y_continuous(labels = scales::comma) +  # Format y-axis labels as integers
  #   theme_minimal()
  # 
  # # Number of accidents per day of the week
  # # Convert CRASH_DATE to datetime and extract day of the week using SQL functions
  collisions <- collisions %>%
    mutate(CRASH_DATE = to_date(CRASH_DATE, 'MM/dd/yyyy')) %>%
    mutate(DAY_OF_WEEK = sql("DATE_FORMAT(CRASH_DATE, 'EEEE')"))

  # # Calculate the number of accidents for each day of the week
  # accidents_per_day <- collisions %>%
  #   group_by(DAY_OF_WEEK) %>%
  #   summarise(total_accidents = n()) %>%
  #   collect()
  # 
  # # Create the correct ordering for days of the week
  # day_levels <- c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
  # 
  # # Convert DAY_OF_WEEK to a factor with the specified levels
  # accidents_per_day <- accidents_per_day %>%
  #   mutate(DAY_OF_WEEK = factor(DAY_OF_WEEK, levels = day_levels))
  # 
  # # Plot the number of accidents for each day of the week
  # ggplot(accidents_per_day, aes(x = DAY_OF_WEEK, y = total_accidents)) +
  #   geom_bar(stat = "identity", fill = "#009999") +
  #   labs(title = "Number of Accidents Per Day of the Week",
  #        x = "Day of the Week",
  #        y = "Total Accidents") +
  #   scale_y_continuous(labels = scales::comma) +
  #   theme_minimal()
  # 

  # 
  # # Categorize each day as either "Weekday" or "Weekend"
# 
#   # Mutate the data to create the WEEKEND column
  collisions <- collisions %>%
    mutate(WEEKEND = if_else(DAY_OF_WEEK %in% c("Saturday", "Sunday"), 1, 0))

#   # Count number of accidents for weekdays and weekends
#   accidents_per_category <- collisions %>%
#     group_by(WEEKEND) %>%
#     summarise(total_accidents = n()) %>%
#     collect()
#   
#   # Calculate the percentage
#   accidents_per_category <- accidents_per_category %>%
#     mutate(percentage = total_accidents / sum(total_accidents) * 100)
#   
#   # Plot the data as a pie chart with percentages
#   ggplot(accidents_per_category, aes(x = "", y = total_accidents, fill = factor(WEEKEND))) +
#     geom_bar(width = 1, stat = "identity") +
#     coord_polar("y") +
#     geom_text(aes(label = paste0(round(percentage, 1), "%")),
#               position = position_stack(vjust = 0.5)) +
#     labs(title = "Proportion of Accidents: Weekdays vs Weekends") +
#     scale_fill_manual(values = c("0" = "orange", "1" = "red"), labels = c("Weekday", "Weekend")) +
#     theme_void() +
#     theme(legend.title = element_blank())
#   
  # ###############################################################
  # # #Place based analysis
  # # # Aggregate the data to count the number of accidents at each location
  # # accident_counts <- collisions %>%
  # #   group_by(LATITUDE, LONGITUDE) %>%
  # #   summarise(total_accidents = n(), .groups = 'drop') %>%
  # #   collect() 
  # # 
  # # hmap <- leaflet() %>%
  # #   addTiles() %>%
  # #   setView(lng = mean(accident_counts$LONGITUDE), lat = mean(accident_counts$LATITUDE), zoom = 11) %>%
  # #   addHeatmap(lng = accident_counts$LONGITUDE, lat = accident_counts$LATITUDE, 
  # #              intensity = accident_counts$total_accidents, radius = 7, blur = 11)
  # # 
  # # hmap
  # 
  # #######################################
  # # Aggregate the count of injuries and fatalities by borough
  # borough_summary <- collisions %>%
  #   group_by(BOROUGH) %>%
  #   summarise(
  #     total_injuries = sum(NUMBER_OF_PERSONS_INJURED),
  #     total_fatalities = sum(NUMBER_OF_PERSONS_KILLED)
  #   ) %>%
  #   collect()
  # 
  # # Plot injuries and fatalities per borough
  # ggplot(borough_summary, aes(x = BOROUGH)) +
  #   geom_bar(aes(y = total_injuries, fill = "Total Injuries"), stat = "identity", alpha = 0.7) +
  #   geom_bar(aes(y = -total_fatalities, fill = "Total Fatalities"), stat = "identity", alpha = 0.7) +
  #   scale_y_continuous(labels = abs) +
  #   labs(title = "Injuries and Fatalities by Borough",
  #        x = "Borough",
  #        y = "Count",
  #        fill = "Type") +
  #   theme_minimal() +
  #   scale_fill_manual(values = c("Total Injuries" = "blue", "Total Fatalities" = "red"))
  # 
  # # Aggregate the count of killed persons by borough and type
  # deaths_by_borough <- collisions %>%
  #   group_by(BOROUGH) %>%
  #   summarise(
  #     PEDESTRIANS_KILLED = sum(NUMBER_OF_PEDESTRIANS_KILLED, na.rm = TRUE),
  #     CYCLISTS_KILLED = sum(NUMBER_OF_CYCLIST_KILLED, na.rm = TRUE),
  #     MOTORISTS_KILLED = sum(NUMBER_OF_MOTORIST_KILLED, na.rm = TRUE)
  #   ) %>%
  #   pivot_longer(cols = c(PEDESTRIANS_KILLED, CYCLISTS_KILLED, MOTORISTS_KILLED), 
  #                names_to = "Type", 
  #                values_to = "Count")
  # 
  # # Plot the number of killed persons by borough
  # ggplot(deaths_by_borough, aes(x = BOROUGH, y = Count, fill = Type)) +
  #   geom_bar(stat = "identity", position = "stack") +
  #   labs(title = "Number of Persons Killed by Borough",
  #        x = "Borough",
  #        y = "Total Deaths",
  #        fill = "Type of Killed Person") +
  #   scale_fill_manual(values = c("PEDESTRIANS_KILLED" = "red", 
  #                                "CYCLISTS_KILLED" = "green", 
  #                                "MOTORISTS_KILLED" = "blue")) +
  #   theme_minimal()
  # 
  # # Aggregate the count of injured persons by borough and type
  # injuries_by_borough <- collisions %>%
  #   group_by(BOROUGH) %>%
  #   summarise(
  #     PEDESTRIANS_INJURED = sum(NUMBER_OF_PEDESTRIANS_INJURED, na.rm = TRUE),
  #     CYCLISTS_INJURED = sum(NUMBER_OF_CYCLIST_INJURED, na.rm = TRUE),
  #     MOTORISTS_INJURED = sum(NUMBER_OF_MOTORIST_INJURED, na.rm = TRUE)
  #   ) %>%
  #   pivot_longer(cols = c(PEDESTRIANS_INJURED, CYCLISTS_INJURED, MOTORISTS_INJURED),
  #                names_to = "Type",
  #                values_to = "Count")
  # 
  # # Plot the number of injured persons by borough
  # ggplot(injuries_by_borough, aes(x = BOROUGH, y = Count, fill = Type)) +
  #   geom_bar(stat = "identity", position = "stack") +
  #   labs(title = "Number of Persons Injured by Borough",
  #        x = "Borough",
  #        y = "Total Injuries",
  #        fill = "Type of Injured Person") +
  #   scale_fill_manual(values = c("PEDESTRIANS_INJURED" = "red",
  #                                "CYCLISTS_INJURED" = "green",
  #                                "MOTORISTS_INJURED" = "blue")) +
  #   theme_minimal()
  # # 
  # 
  
  
  #############HEAT MAPA
  
  # 
  # # Count accidents by hour and day of week
  # accidents_hour_day <- collisions %>%
  #   group_by(CRASH_HOUR, DAY_OF_WEEK) %>%
  #   summarise(total_accidents = n()) %>%
  #   collect()
  # 
  # # Plot heatmap
  # ggplot(accidents_hour_day, aes(x = CRASH_HOUR, y = DAY_OF_WEEK, fill = total_accidents)) +
  #   geom_tile(color = "white") +
  #   scale_fill_gradient(low = "blue", high = "red") +
  #   labs(title = "Heatmap of Accidents by Hour and Day of Week",
  #        x = "Hour of Day", y = "Day of Week", fill = "Total Accidents") +
  #   theme_minimal()
  # 
  # 
  # #######################################
  
  # #Korelacije
  #install.packages("corrplot")
  # library(corrplot)
  # library(reshape2)
  # # 
  # cols_of_interest <- c("NUMBER_OF_PERSONS_INJURED",
  #                       "NUMBER_OF_PERSONS_KILLED", "NUMBER_OF_PEDESTRIANS_INJURED",
  #                       "NUMBER_OF_PEDESTRIANS_KILLED", "NUMBER_OF_CYCLIST_INJURED",
  #                       "NUMBER_OF_CYCLIST_KILLED", "NUMBER_OF_MOTORIST_INJURED",
  #                       "NUMBER_OF_MOTORIST_KILLED")
  # numeric_data <- collisions %>%
  #   select(all_of(cols_of_interest)) %>%
  #   as.data.frame()
  # 
  # # Your correlation matrix calculation (assuming the data is already cleaned and prepared)
  # correlation_matrix <- cor(numeric_data, use = "complete.obs")
  # 
  # # Plot the correlation matrix with a reduced text size
  # corrplot(correlation_matrix, method = "circle", type = "lower", tl.cex = 0.7)
  # 
  # 
  # melted_correlation_matrix <- melt(correlation_matrix)
  # 
  # # Plot the heatmap
  # ggplot(data = melted_correlation_matrix, aes(x=Var1, y=Var2, fill=value)) +
  #   geom_tile(color = "white") +
  #   scale_fill_gradient2(low = "blue", high = "red", mid = "white",
  #                        midpoint = 0, limit = c(-1,1), space = "Lab",
  #                        name="Correlation") +
  #   theme_minimal() +
  #   theme(axis.text.x = element_text(angle = 45, vjust = 1,
  #                                    size = 12, hjust = 1),
  #         axis.text.y = element_text(size = 12),
  #         plot.title = element_text(size = 15, hjust = 0.5),
  #         plot.subtitle = element_text(size = 10, hjust = 0.5),
  #         plot.caption = element_text(size = 8)) +
  #   geom_text(aes(label = round(value, 2)), color = "black", size = 3) +
  #   labs(title = "Correlation Matrix Heatmap",
  #        subtitle = "Showing correlation coefficients for all variables",
  #        caption = "Data source: NYC collisions data") +
  #   coord_fixed()
  # 
  # # Save plot to file for better resolution
  # ggsave("correlation_heatmap.png", width = 12, height = 9)
  # 
  # 
  # # Extract hour and day of the week from CRASH_DATE and CRASH_TIME
  # 

  
  # ###########################

  collisions %>%
     group_by(NUMBER_OF_PERSONS_KILLED) %>%
      summarise(total_accidents = n()) %>%
      collect()

  collisions %>%
    group_by(NUMBER_OF_PERSONS_INJURED) %>%
    summarise(total_accidents = n()) %>%
    collect()
  

  # Pretvaranje kategoričkog obeležja 'DAY_OF_WEEK' u numeričko
  cleaned_data <- collisions %>%
    mutate(DAY_OF_WEEK = case_when(
      DAY_OF_WEEK == "Monday" ~ 1,
      DAY_OF_WEEK == "Tuesday" ~ 2,
      DAY_OF_WEEK == "Wednesday" ~ 3,
      DAY_OF_WEEK == "Thursday" ~ 4,
      DAY_OF_WEEK == "Friday" ~ 5,
      DAY_OF_WEEK == "Saturday" ~ 6,
      DAY_OF_WEEK == "Sunday" ~ 7,
      TRUE ~ 0  # Handle any other cases
    ))
  
  # Pretvaranje kategoričkog obeležja 'BOROUGH' u numeričko
  cleaned_data <- cleaned_data %>%
    mutate(BOROUGH_LABELED = case_when(
      BOROUGH == "BROOKLYN" ~ 1,
      BOROUGH == "BRONX" ~ 2,
      BOROUGH == "MANHATTAN" ~ 3,
      BOROUGH == "QUEENS" ~ 4,
      BOROUGH == "STATEN ISLAND" ~ 5,
      TRUE ~ 0  # Handle any other cases
    ))
  cleaned_data <- cleaned_data %>% 
    select(-BOROUGH)


  # Stvaranje ciljnog obeležja koje govori da li je neko bio ubijen i/ili povredjen
  cleaned_data <- cleaned_data %>%
    mutate(Y = case_when(
      (NUMBER_OF_PERSONS_KILLED == 0) & (NUMBER_OF_PERSONS_INJURED == 0) ~ 'No',
      (NUMBER_OF_PERSONS_KILLED >= 1) & (NUMBER_OF_PERSONS_INJURED >= 1) ~ 'Yes',
      (NUMBER_OF_PERSONS_KILLED >= 1) & (NUMBER_OF_PERSONS_INJURED == 0) ~ 'No',
      (NUMBER_OF_PERSONS_KILLED == 0) & (NUMBER_OF_PERSONS_INJURED >= 1) ~ 'Yes'
    ))
  
  cleaned_data <- cleaned_data %>% 
    select(-NUMBER_OF_PERSONS_KILLED)
  cleaned_data <- cleaned_data %>% 
    select(-NUMBER_OF_PERSONS_INJURED)

  cleaned_data <- cleaned_data %>% 
    select(-CRASH_DATE)
  cleaned_data <- cleaned_data %>% 
    select(-CRASH_TIME)
  
  class_separation<-cleaned_data %>%
    group_by(Y) %>%
    summarise(total_accidents = n()) %>%
    collect()

  ggplot(class_separation, aes(x = factor(Y, labels = c("No", "Yes")), y = total_accidents)) +
    geom_bar(stat = "identity", color = "darkgray") +
    labs(title = "Distribution of Accidents by Fatalities",
         x = "Anybody Killed/Injured",
         y = "Number of Accidents") +
    scale_y_continuous(labels = scales::number_format()) +
    scale_x_discrete(labels = c("No", "Yes")) +
    theme_minimal()
  ############################### 
  
  
  
  
  # Aggregate the data
  # Aggregate the data using sparklyr functions
  agg_data <- cleaned_data %>%
    group_by(Y, BOROUGH_LABELED) %>%
    summarise(count = n()) %>%
    collect()  # Collect the aggregated data to the local machine
  
  # Print the aggregated data
  print(agg_data)

  # Create a bar plot
  ggplot(agg_data, aes(x = BOROUGH_LABELED, y = count, fill = Y)) +
    geom_bar(stat = "identity") +
    labs(title = "Count of Y for Each BOROUGH_LABELED", x = "BOROUGH_LABELED", y = "Count") +
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    scale_fill_manual(values = c("No" = "blue", "Yes" = "red"))
  
  
  # ggplot(collisions, aes(x = as.factor(Y), y = BOROUGH_LABELED)) +
  #   geom_boxplot() +
  #   labs(title = "Y vs. Borough", x = "Y", y = "Borough")
  # 
  # 
  

  
  # Assuming `cleaned_data` is your Spark DataFrame
  # Assuming `sc` is your Spark connection
  
  target_col <- "Y"
  
  # Select the categorical variables you want to test for association
  
  #Chi test za vremenska kategorička obeležja
  categorical_vars <- c("DAY_OF_WEEK", "CRASH_MONTH", "CRASH_HOUR", "WEEKEND")
  
  # Loop through each categorical variable
  for (var in categorical_vars) {
    # Create contingency table between the categorical variable and the target variable
    contingency_table <- cleaned_data %>%
      select(all_of(var), all_of(target_col)) %>%
      collect() %>%
      table()
    
    # Perform chi-square test
    chi_square_result <- chisq.test(contingency_table)
    
    # Print results
    cat("Contigency table between", var, "and", target_col, ":\n")
    print(contingency_table)
    cat("\n")
    
    # Print results
    cat("Chi-square test between", var, "and", target_col, ":\n")
    print(chi_square_result)
    cat("\n")
  }
  
  #Chi test za prostorna kategorička obeležja
  categorical_vars <- c("BOROUGH_LABELED","LONGITUDE","LATITUDE")
  
  # Loop through each categorical variable
  for (var in categorical_vars) {
    # Create contingency table between the categorical variable and the target variable
    contingency_table <- cleaned_data %>%
      select(all_of(var), all_of(target_col)) %>%
      collect() %>%
      table()
    
    # Perform chi-square test
    chi_square_result <- chisq.test(contingency_table)
    
    # Print results
    cat("Contigency table between", var, "and", target_col, ":\n")
    print(contingency_table)
    cat("\n")
    
    # Print results
    cat("Chi-square test between", var, "and", target_col, ":\n")
    print(chi_square_result)
    cat("\n")
  }
  
  
  #Chi test za ostala kategorička obeležja
  categorical_vars <- c("CONTRIBUTING_FACTOR_VEHICLE_1","CLEANED_VEHICLE_TYPE")
  
  # Loop through each categorical variable
  for (var in categorical_vars) {
    # Create contingency table between the categorical variable and the target variable
    contingency_table <- cleaned_data %>%
      select(all_of(var), all_of(target_col)) %>%
      collect() %>%
      table()
    
    # Perform chi-square test
    chi_square_result <- chisq.test(contingency_table)
    
    # Print results
    cat("Contigency table between", var, "and", target_col, ":\n")
    print(contingency_table)
    cat("\n")
    
    # Print results
    cat("Chi-square test between", var, "and", target_col, ":\n")
    print(chi_square_result)
    cat("\n")
  }
  
  
  #Može se izbaciti obeležje 'WEEKEND' s obzirom da postoji obeležje 'DAY_OF_WEEK' koje ima bolje rezultate testova, a svejedno ne gubimo informacije.
  # Slično i za ostale.
  # cleaned_data <- cleaned_data %>% 
  #   select(-ZIP_CODE)
  # cleaned_data <- cleaned_data %>% 
  #   select(-LATITUDE)
  # cleaned_data <- cleaned_data %>% 
  #   select(-LONGITUDE)
  

  