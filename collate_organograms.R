library(jsonlite)
library(tidyverse)
library(httr)
library(lubridate)

datset_json_dump <- 'http://www.data.gov.uk/data/dumps/data.gov.uk-ckan-meta-data-latest.json.zip'
data_folder <- 'data'

#need to sort the unzip

download_csv <- function(url, destination_file){
  if(!file.exists(destination_file)){
    try(download.file(url, destination_file, quite = T))
  }
  return(!file.info(destination_file)$size == 0)
}

meta_data <- 'data.gov.uk-ckan-meta-data-2017-12-22.json' %>%
  fromJSON(flatten = T)

organogram_meta_data <- meta_data %>%
  filter(grepl('organogram', tolower(title))) %>%
  mutate(resources = map(resources, bind_rows)) %>%
  unnest(resources) %>%
  filter(format == 'CSV') %>%
  mutate(destination_file = map_chr(id1, function(x,y) paste(data_folder, '//', x, '.csv', sep = ''))) %>%
  mutate(data_exists = map2_lgl(url1, destination_file, download_csv)) %>%
  filter(data_exists) %>%
  mutate(is_junior = grepl('junior', tolower(description)),
         is_senior = grepl('senior|scs', tolower(description))) %>%
  filter(is_junior + is_senior == 1)

all_organogram_data <- organogram_meta_data %>% 
  mutate(file_contents = map(destination_file, ~ read.csv(., row.names = NULL))) %>%
  mutate(file_contents = map(file_contents, ~ mutate_all(.,as.character))) %>%
  mutate(date = parse_date_time(date, c('Ymd', 'mY', 'Ym'))) %>%
  select(id1, destination_file, date, is_junior, is_senior)

junior_data <- all_organogram_data %>%
  filter(is_junior) %>%
  unnest(file_contents) %>%
  transmute(date = date,
            file_id = id1,
            destination_file = destination_file,
            parent_department = Parent.department,
            organisation = Organisation,
            unit = Unit,
            reporting_senior_post = pmax(Reporting.Senior.Post, Reporting.SCS.post.reference, na.rm = T),
            grade = Grade,
            payscale_minimum_gbp = as.numeric(pmax(Payscale.Minimum..Â.., Payscale.Minimum...., Payscale.minimum, na.rm = T)),
            payscale_maximum_gbp = as.numeric(pmax(Payscale.Maximum..Â.., Payscale.Maximum...., Payscale.maximum, na.rm = T)),
            generic_job_title = pmax(Generic.Job.Title, Generic.job.title, na.rm = T),
            number_of_posts_fte = as.numeric(pmax(Number.of.Posts.in.FTE, Number.of.such.posts.in.FTE, Number.of.such.posts.FTE, na.rm = T)),
            professional_occupational_group = pmax(Professional.Occupational.Group, Profession, na.rm = T))

senior_data <- all_organogram_data %>%
  filter(is_senior) %>%
  unnest(file_contents) %>%
  transmute(date = date,
            file_id = id1,
            destination_file = destination_file,
            parent_department = pmax(Parent.Department, Parent.Department.Sponsor.Department, na.rm = T),
            organisation = Organisation,
            post_unique_reference = pmax(Post.Unique.Reference, Post.unique.reference, Unique.reference.number, na.rm = T),
            person_role_name = Name,
            grade_or_equivalent = pmax(Grade..or.equivalent., Grade, GRADE, na.rm = T),
            job_title = pmax(Job.Title, Job.title, na.rm = T),
            job_team_function = pmax(Job.Team.Function, Job.Function, Job...team.function, na.rm = T),
            unit = Unit,
            contact_phone = pmax(Contact.Phone, Contact.phone, na.rm = T),
            contact_email = pmax(Contact.E.mail, Contact.e.mail, E.mail, Contact.Email, na.rm = T),
            reports_to = pmax(Reports.to.Senior.Post, Reports.To, Reports.to, Reporting.Senior.Post, na.rm = T),
            salary_of_reports_gbp = as.numeric(pmax(Salary.Cost.of.Reports..Â.., Salary.Cost.of.Reports...., Salary.Cost.of.Reports, Salary.Costs.of.Reports, Salary.cost.of.reports, salary.costs.of.reports, na.rm = T)),
            fte = as.numeric(pmax(FTE, Number.of.Posts.in.FTE, na.rm = T)),
            actual_pay_floor_gbp = as.numeric(pmax(Actual.Pay.Floor..Â.., Actual.pay.floor, Actual.Pay.Floor...., Actual.Pay.Floor, Payscale.Minimum...., na.rm = T)),
            actual_pay_ceiling_gbp = as.numeric(pmax(Actual.Pay.Ceiling..Â.., Actual.Pay.Ceiling...., Actual.Pay.Ceiling, E.mail, na.rm = T)),
            professional_occupational_group = pmax(Professional.Occupational.Group, Profession, na.rm = T),
            notes = Notes)

write_tsv(junior_data, 'junior_data.csv')
write_tsv(senior_data, 'senior_data.csv')

