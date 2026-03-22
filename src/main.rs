use reqwest::Client;
use scraper::{Html, Selector};
use regex::Regex;
use url::Url;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_urls = vec![
        "https://www.janestreet.com/join-jane-street/",
        "https://www.citadelsecurities.com/careers/",
        "https://www.twosigma.com/careers/",
        "https://www.deshaw.com/careers",
        "https://www.hudsonrivertrading.com/careers/",
        "https://www.virtu.com/careers/",
        "https://www.jumptrading.com/careers/",
        "https://www.imc.com/eu/careers/",
        "https://optiver.com/working-at-optiver/career-opportunities/",
        "https://akunacapital.com/careers",
        "https://careers.sig.com/",
        "https://drw.com/work-at-drw/",
        "https://www.flowtraders.com/careers",
        "https://fiverings.com/apply/",
        "https://www.rentec.com/Careers.action",
        "https://www.citadel.com/careers/",
        "https://www.mlp.com/careers/",
        "https://careers.point72.com/",
        "https://www.bridgewater.com/careers/",
        "https://www.aqr.com/About-Us/Careers",
        "https://www.man.com/careers",
        "https://www.winton.com/careers",
        "https://www.worldquant.com/career-listing/",
        "https://www.pdtpartners.com/careers.html",
        "https://www.tgsmanagement.com/careers",
        "https://www.squarepointcap.com/careers",
        "https://www.balyasny.com/careers",
        "https://www.exoduspoint.com/careers/",
        "https://www.goldmansachs.com/careers/",
        "https://www.morganstanley.com/people-opportunities",
        "https://careers.jpmorgan.com/",
        "https://search.jobs.barclays/",
        "https://careers.db.com/",
        "https://careers.bankofamerica.com/",
        "https://www.ubs.com/global/en/careers.html",
        "https://www.tower-research.com/open-positions",
        "https://www.xrtrading.com/careers",
        "https://www.genevatrading.com/careers",
        "https://www.belvederetrading.com/careers",
        "https://www.transmarketgroup.com/careers",
        "https://www.radixtrading.com/",
        "https://www.vaticlabs.com/",
        "https://www.headlandstech.com/careers",
        "https://www.gresearch.co.uk/careers/",
        "https://www.qube-rt.com/careers/",
        "https://www.systematica.com/careers",
        "https://www.wintermute.com/careers",
        "https://www.gsr.io/careers/",
        "https://www.ambergroup.io/careers",
        "https://keyrock.eu/careers",
        "https://www.b2c2.com/careers/",
        "https://deepmind.google/careers/",
        "https://www.metacareers.com/",
        "https://careers.robinhood.com/",
        "https://www.coinbase.com/careers",
    ];

    let max_depth = 5;
    let mut visited_urls = HashSet::new();
    let mut found_emails = HashSet::new();
    
    let mut current_level_urls = Vec::new();
    for url in start_urls {
        current_level_urls.push(url.to_string());
        visited_urls.insert(url.to_string());
    }

    let client = Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let email_regex = Arc::new(Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap());
    let a_selector = Arc::new(Selector::parse("a").unwrap());

    let mut emails_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("emails.txt")?;

    println!("Starting multi-threaded crawl on {} URLs with max depth {}", current_level_urls.len(), max_depth);

    // Limit the number of concurrent requests to prevent resource exhaustion
    let concurrency_limiter = Arc::new(Semaphore::new(50));

    for depth in 0..=max_depth {
        if current_level_urls.is_empty() {
            break;
        }

        println!("--- Crawling depth {} with {} URLs ---", depth, current_level_urls.len());

        let mut join_set = JoinSet::new();

        for url in current_level_urls.drain(..) {
            let client = client.clone();
            let email_regex = Arc::clone(&email_regex);
            let a_selector = Arc::clone(&a_selector);
            let extract_links = depth < max_depth;
            let permit = concurrency_limiter.clone().acquire_owned().await.unwrap();

            join_set.spawn(async move {
                println!("Fetching: {}", url);
                let result = process_url(url.clone(), client, email_regex, a_selector, extract_links).await;
                drop(permit);
                (url, result)
            });
        }

        let mut next_level_urls = Vec::new();

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok((_url, (emails, new_links))) => {
                    for email in emails {
                        if found_emails.insert(email.clone()) {
                            println!("Found new email: {}", email);
                            if let Err(e) = writeln!(emails_file, "{}", email) {
                                eprintln!("Failed to write to file: {}", e);
                            }
                        }
                    }
                    for link in new_links {
                        if visited_urls.insert(link.clone()) {
                            next_level_urls.push(link);
                        }
                    }
                }
                Err(e) => eprintln!("Task failed: {}", e),
            }
        }

        current_level_urls = next_level_urls;
    }

    println!("Crawling finished. Found {} unique emails.", found_emails.len());

    Ok(())
}

async fn process_url(
    url: String, 
    client: Client, 
    email_regex: Arc<Regex>, 
    a_selector: Arc<Selector>,
    extract_links: bool
) -> (Vec<String>, Vec<String>) {
    let mut emails = Vec::new();
    let mut links = Vec::new();

    let response = match client.get(&url).send().await {
        Ok(res) => res,
        Err(_) => return (emails, links),
    };

    if !response.status().is_success() {
        return (emails, links);
    }

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();

    if !content_type.contains("text/html") && !content_type.contains("text/plain") {
        return (emails, links);
    }

    let body = match response.text().await {
        Ok(b) => b,
        Err(_) => return (emails, links),
    };

    // Extract emails
    for captures in email_regex.captures_iter(&body) {
        if let Some(email_match) = captures.get(0) {
            emails.push(email_match.as_str().to_string());
        }
    }

    if extract_links && content_type.contains("text/html") {
        if let Ok(base_url) = Url::parse(&url) {
            let document = Html::parse_document(&body);
            for element in document.select(&a_selector) {
                if let Some(href) = element.value().attr("href") {
                    if let Ok(mut joined_url) = base_url.join(href) {
                        if joined_url.scheme() == "http" || joined_url.scheme() == "https" {
                            let same_domain = match (base_url.host_str(), joined_url.host_str()) {
                                (Some(base), Some(joined)) => {
                                    let b = base.trim_start_matches("www.");
                                    let j = joined.trim_start_matches("www.");
                                    b == j || j.ends_with(&format!(".{}", b)) || b.ends_with(&format!(".{}", j))
                                }
                                _ => false,
                            };

                            if same_domain {
                                joined_url.set_fragment(None);
                                let mut next_url = joined_url.to_string();
                                if next_url.ends_with('/') {
                                    next_url.pop();
                                }
                                links.push(next_url);
                            }
                        }
                    }
                }
            }
        }
    }

    (emails, links)
}
