levels_job_title <- c(
    "Analyst",
    "Data Engineer",
    "Data Scientist",
    "ML Engineer",
    "Research",
    "Architecture",
    "Management",
    "Consulting",
    "BI Developer",
    "AI Developer",
    "Senior",
    "Specialist",
    "Operations"
  )

mapping_job_title <- c(
  # Data Analysis & BI
  "Data Analyst" = "Analyst",
  "Business Data Analyst" = "Analyst",
  "BI Data Analyst" = "Analyst",
  "Product Data Analyst" = "Analyst",
  "Marketing Data Analyst" = "Analyst",
  "Finance Data Analyst" = "Analyst",
  "Financial Data Analyst" = "Analyst",
  "Compliance Data Analyst" = "Analyst",
  "Data Quality Analyst" = "Analyst",
  "Insight Analyst" = "Analyst",
  "BI Analyst" = "Analyst",
  "Data Operations Analyst" = "Analyst",

  # Data Engineering
  "Data Engineer" = "Data Engineer",
  "Big Data Engineer" = "Data Engineer",
  "Machine Learning Infrastructure Engineer" = "Data Engineer",
  "Marketing Data Engineer" = "Data Engineer",
  "Cloud Data Engineer" = "Data Engineer",
  "Data Analytics Engineer" = "Data Engineer",
  "Data Science Engineer" = "Data Engineer",
  "BI Data Engineer" = "Data Engineer",
  "Data Infrastructure Engineer" = "Data Engineer",
  "ETL Engineer" = "Data Engineer",
  "ETL Developer" = "Data Engineer",
  "Software Data Engineer" = "Data Engineer",
  "Azure Data Engineer" = "Data Engineer",
  "Data DevOps Engineer" = "Data Engineer",
  "Cloud Database Engineer" = "Data Engineer",
  "Analytics Engineer" = "Data Engineer",

  # Data Science & ML
  "Data Scientist" = "Data Scientist",
  "Applied Data Scientist" = "Data Scientist",
  "Product Data Scientist" = "Data Scientist",
  "Machine Learning Scientist" = "Data Scientist",
  "Applied Machine Learning Scientist" = "Data Scientist",
  "AI Scientist" = "Data Scientist",

  # Machine Learning Engineering
  "Machine Learning Engineer" = "ML Engineer",
  "ML Engineer" = "ML Engineer",
  "Computer Vision Engineer" = "ML Engineer",
  "Machine Learning Developer" = "ML Engineer",
  "Machine Learning Research Engineer" = "ML Engineer",
  "Computer Vision Software Engineer" = "ML Engineer",
  "NLP Engineer" = "ML Engineer",
  "Deep Learning Engineer" = "ML Engineer",
  "Applied Machine Learning Engineer" = "ML Engineer",
  "Machine Learning Software Engineer" = "ML Engineer",
  "MLOps Engineer" = "ML Engineer",

  # Research & Advanced
  "Research Scientist" = "Research",
  "Research Engineer" = "Research",
  "Machine Learning Researcher" = "Research",
  "3D Computer Vision Researcher" = "Research",
  "Deep Learning Researcher" = "Research",
  "Applied Scientist" = "Research",

  # Architecture & Specialized
  "Data Architect" = "Architecture",
  "Big Data Architect" = "Architecture",
  "Cloud Data Architect" = "Architecture",
  "Data Modeler" = "Architecture",
  "Principal Data Architect" = "Architecture",

  # Management & Leadership
  "Director of Data Science" = "Management",
  "Data Science Manager" = "Management",
  "Machine Learning Manager" = "Management",
  "Head of Data Science" = "Management",
  "Head of Data" = "Management",
  "Head of Machine Learning" = "Management",
  "Data Science Lead" = "Management",
  "Data Scientist Lead" = "Management",
  "Data Analytics Manager" = "Management",
  "Data Manager" = "Management",
  "Manager Data Management" = "Management",
  "Data Lead" = "Management",

  # Consulting & Strategy
  "Data Science Consultant" = "Consulting",
  "Data Analytics Consultant" = "Consulting",
  "Data Strategist" = "Consulting",

  # BI Development
  "Power BI Developer" = "BI Developer",
  "BI Developer" = "BI Developer",
  "Business Intelligence Engineer" = "BI Developer",

  # AI Development
  "AI Developer" = "AI Developer",
  "AI Programmer" = "AI Developer",

  # Senior/Staff Roles
  "Staff Data Analyst" = "Senior",
  "Lead Data Analyst" = "Senior",
  "Lead Data Engineer" = "Senior",
  "Lead Data Scientist" = "Senior",
  "Lead Machine Learning Engineer" = "Senior",
  "Staff Data Scientist" = "Senior",
  "Principal Data Scientist" = "Senior",
  "Principal Data Analyst" = "Senior",
  "Principal Data Engineer" = "Senior",
  "Principal Machine Learning Engineer" = "Senior",
  "Data Science Tech Lead" = "Senior",
  "Data Analytics Lead" = "Senior",

  # Specialized & Other
  "Data Specialist" = "Specialist",
  "Data Analytics Specialist" = "Specialist",
  "Data Management Specialist" = "Specialist",
  "Autonomous Vehicle Technician" = "Specialist",
  "Data Operations Engineer" = "Operations"
)


mapping_employement_type <- c(
    "FT" = "Full-time",
    "PT" = "Part-time",
    "CT" = "Contract",
    "FL" = "Freelance"
  )


levels_employement_type <- c("Full-time", "Part-time", "Contract", "Freelance", "Unknown")


region_map <- c(
      "AL"="Europe","AM"="Asia","AT"="Europe","BA"="Europe",
      "BE"="Europe","BG"="Europe","BY"="Europe","CH"="Europe",
      "CY"="Europe","CZ"="Europe","DE"="Europe","DK"="Europe",
      "EE"="Europe","ES"="Europe","FI"="Europe","FR"="Europe",
      "GB"="Europe","GR"="Europe","HR"="Europe","HU"="Europe",
      "IE"="Europe","IS"="Europe","IT"="Europe","JE"="Europe",
      "LT"="Europe","LU"="Europe","LV"="Europe","MD"="Europe",
      "MK"="Europe","MT"="Europe",
      "NL"="Europe","NO"="Europe",
      "PL"="Europe","PT"="Europe","RO"="Europe","RS"="Europe",
      "RU"="Europe","SE"="Europe","SI"="Europe","SK"="Europe",
      "UA"="Europe",
      "AR"="South America","BO"="South America","BR"="South America",
      "BS"="North America","CA"="North America",
      "CL"="South America","CO"="South America","CR"="North America",
      "DO"="North America",
      "MX"="North America",
      "PR"="North America","US"="North America","VE"="South America",
      "UY"="South America","PE"="South America","EC"="South America",
      "BZ"="North America","PA"="North America",
      "CF"="Africa","DZ"="Africa","EG"="Africa","GH"="Africa",
      "KE"="Africa","MA"="Africa","NG"="Africa","TN"="Africa",
      "ZA"="Africa","CI"="Africa","SN"="Africa","TZ"="Africa",
      "UG"="Africa","ET"="Africa","CM"="Africa",
      "AE"="Asia","AS"="Oceania","BD"="Asia","CN"="Asia","HK"="Asia",
      "ID"="Asia","IL"="Asia","IN"="Asia","IQ"="Asia","IR"="Asia",
      "JP"="Asia","KW"="Asia","KR"="Asia","KZ"="Asia","LA"="Asia",
      "MY"="Asia","PH"="Asia","PK"="Asia","QA"="Asia","SA"="Asia",
      "SG"="Asia","TH"="Asia","TR"="Asia","VN"="Asia","XK"="Europe",
      "UZ"="Asia","NZ"="Oceania","AU"="Oceania", "KP"="Asia", "TW"="Asia", "CV"="Africa"
    )


regions_levels <- c("South America", "Europe", "Asia", "North America", "Africa", "Oceania")


levels_iso2 <- c(

    "AL","AM","AT","BA","BE","BG","BY","CH","CY","CZ","DE","DK","EE",
    "ES","FI","FR","GB","GR","HR","HU","IE","IS","IT","JE","LT","LU",
    "LV","MD","MK","MT","NL","NO","PL","PT","RO","RS","RU","SE","SI","SK","UA",
    "AR","BO","BR","BS","CA","CL","CO","CR","DO","MX","PR","US","VE",
    "UY","PE","EC","BZ","PA","HN",
    "CF","EG","GH","KE","NG","ZA","CI","SN","TZ","UG","ET","CM","DZ","TN","MA",
    "AE","AS","BD","CN","HK","ID","IL","IN","IQ","IR","JP","KW","KR","KZ","LA",
    "MY","PH","PK","QA","SA","SG","TH","TR","VN","XK","UZ","NZ",
    "AU", "KP", "TW", "CV"
  )


mapping_total <- c(
    "KP"="KP", "TW"="TW", "CV"="CV", "HN"="HN",
    "AL" = "AL", "AM" = "AM", "AT" = "AT", "BA" = "BA", "BE" = "BE", 
    "BG" = "BG", "BY" = "BY", "CH" = "CH", "CY" = "CY", "CZ" = "CZ", 
    "DE" = "DE", "DK" = "DK", "EE" = "EE", "ES" = "ES", "FI" = "FI", 
    "FR" = "FR", "GB" = "GB", "GR" = "GR", "HR" = "HR", "HU" = "HU", 
    "IE" = "IE", "IS" = "IS", "IT" = "IT", "JE" = "JE", "LT" = "LT", 
    "LU" = "LU", "LV" = "LV", "MD" = "MD", "MK" = "MK", "MT" = "MT", 
    "NL" = "NL", "NO" = "NO", "PL" = "PL", "PT" = "PT", "RO" = "RO", 
    "RS" = "RS", "RU" = "RU", "SE" = "SE", "SI" = "SI", "SK" = "SK", 
    "UA" = "UA", "AR" = "AR", "BO" = "BO", "BR" = "BR", "BS" = "BS", 
    "CA" = "CA", "CL" = "CL", "CO" = "CO", "CR" = "CR", "DO" = "DO", 
    "MX" = "MX", "PR" = "PR", "US" = "US", "VE" = "VE", "UY" = "UY", 
    "PE" = "PE", "EC" = "EC", "BZ" = "BZ", "PA" = "PA", "CF" = "CF", 
    "EG" = "EG", "GH" = "GH", "KE" = "KE", "NG" = "NG", "ZA" = "ZA", 
    "CI" = "CI", "SN" = "SN", "TZ" = "TZ", "UG" = "UG", "ET" = "ET", 
    "CM" = "CM", "DZ" = "DZ", "TN" = "TN", "MA" = "MA", "AE" = "AE", 
    "AS" = "AS", "BD" = "BD", "CN" = "CN", "HK" = "HK", "ID" = "ID", 
    "IL" = "IL", "IN" = "IN", "IQ" = "IQ", "IR" = "IR", "JP" = "JP", 
    "KW" = "KW", "KR" = "KR", "KZ" = "KZ", "LA" = "LA", "MY" = "MY", 
    "PH" = "PH", "PK" = "PK", "QA" = "QA", "SA" = "SA", "SG" = "SG", 
    "TH" = "TH", "TR" = "TR", "VN" = "VN", "XK" = "XK", "UZ" = "UZ", 
    "NZ" = "NZ", "AU" = "AU",
    "UNITED STATES"="US","UNITED KINGDOM"="GB","GREAT BRITAIN"="GB",
    "ENGLAND"="GB", "FRANCE" = "FR",
    "CZECH REPUBLIC"="CZ","CZECHIA"="CZ","RUSSIA"="RU","REPUBLIC OF KOREA"="KR",
    "SOUTH KOREA"="KR",
    "NORTH KOREA"="KP","VIETNAM"="VN","VIET NAM"="VN","HONG KONG"="HK",
    "TAIWAN"="TW",
    "UNITED ARAB EMIRATES"="AE","UAE"="AE","SAUDI ARABIA"="SA","TURKEY"="TR",
    "TÜRKIYE"="TR",
    "BOSNIA AND HERZEGOVINA"="BA","NORTH MACEDONIA"="MK","MACEDONIA"="MK",
    "BOLIVIA"="BO",
    "MOLDOVA"="MD","REPUBLIC OF MOLDOVA"="MD","LAOS"="LA","LAO PDR"="LA",
    "CAPE VERDE"="CV",
    "IVORY COAST"="CI","CÔTE D'IVOIRE"="CI","SOUTH AFRICA"="ZA",
    "ARE"="AE","ALB"="AL","ARM"="AM","ARG"="AR","ASM"="AS","AUT"="AT",
    "AUS"="AU","BIH"="BA","BEL"="BE",
    "BOL"="BO","BRA"="BR","BHS"="BS","CAN"="CA","CAF"="CF","CHE"="CH",
    "CHL"="CL","CHN"="CN","COL"="CO",
    "CRI"="CR","CYP"="CY","CZE"="CZ","DEU"="DE","DNK"="DK","DOM"="DO",
    "DZA"="DZ","EST"="EE","EGY"="EG",
    "ESP"="ES","FIN"="FI","FRA"="FR","GBR"="GB","GHA"="GH","GRC"="GR",
    "HKG"="HK","HND"="HN","HRV"="HR",
    "HUN"="HU","IDN"="ID","IRL"="IE","ISR"="IL","IND"="IN","IRQ"="IQ",
    "IRN"="IR","ITA"="IT","JEY"="JE",
    "JPN"="JP","KEN"="KE","KWT"="KW","LTU"="LT","LUX"="LU","LVA"="LV",
    "MAR"="MA","MDA"="MD","MKD"="MK",
    "MLT"="MT","MEX"="MX","MYS"="MY","NGA"="NG","NLD"="NL","NZL"="NZ",
    "PHL"="PH","PAK"="PK","POL"="PL",
    "PRI"="PR","PRT"="PT","ROU"="RO","SRB"="RS","RUS"="RU","SWE"="SE",
    "SGP"="SG","SVN"="SI","SVK"="SK",
    "THA"="TH","TUN"="TN","TUR"="TR","UKR"="UA","USA"="US",
    "UZB"="UZ","VNM"="VN","ZAF"="ZA","UK" = "GB",
    "EL" = "GR", "KO" = "XK","XKX" = "XK"
  )


size_mapping <- c(
    'S'= "Small",
    'M'= "Medium", 
    'L'= "Large"
)

#'@export
size_levels <- c("Small", "Medium", "Large")


experience_mapping <- c(
  "EN" = "Entry-level",
  "MI" = "Mid-level", 
  "SE" = "Senior-level",
  "EX" = "Executive-level"
)