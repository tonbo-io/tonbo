//! Nested Types: Deep struct nesting, List, and complex compositions
//!
//! Run: cargo run --example 08_nested_types

use typed_arrow::bridge::List;
use tonbo::prelude::*;

// Level 1: Geo coordinates (innermost)
#[derive(Record, Clone)]
struct Geo {
    lat: f64,
    lon: f64,
}

// Level 2: Address contains optional Geo
#[derive(Record, Clone)]
struct Address {
    city: String,
    zip: Option<i32>,
    geo: Option<Geo>,
}

// Level 3: Company contains optional Address (headquarters)
#[derive(Record, Clone)]
struct Company {
    name: String,
    hq: Option<Address>,
}

// Level 4: Person with deep nesting + List
#[derive(Record)]
struct Person {
    #[metadata(k = "tonbo.key", v = "true")]
    id: i64,
    name: String,
    company: Option<Company>,
    home: Option<Address>,
    tags: Option<List<String>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DbBuilder::from_schema(Person::schema())?
        .on_disk("/tmp/tonbo_nested_types")?
        .open()
        .await?;

    // Create people with varying levels of nested data
    let people = vec![
        // Full deep nesting: Person -> Company -> Address -> Geo
        Person {
            id: 1,
            name: "Alice".into(),
            company: Some(Company {
                name: "TechCorp".into(),
                hq: Some(Address {
                    city: "Seattle".into(),
                    zip: Some(98101),
                    geo: Some(Geo {
                        lat: 47.6062,
                        lon: -122.3321,
                    }),
                }),
            }),
            home: Some(Address {
                city: "Bellevue".into(),
                zip: Some(98004),
                geo: None,
            }),
            tags: Some(List::new(vec!["engineer".into(), "rust".into()])),
        },
        // Partial nesting: company without HQ address
        Person {
            id: 2,
            name: "Bob".into(),
            company: Some(Company {
                name: "StartupInc".into(),
                hq: None, // Remote-first, no HQ
            }),
            home: Some(Address {
                city: "Portland".into(),
                zip: None,
                geo: Some(Geo {
                    lat: 45.5152,
                    lon: -122.6784,
                }),
            }),
            tags: Some(List::new(vec!["founder".into()])),
        },
        // Minimal data: no company, no home
        Person {
            id: 3,
            name: "Carol".into(),
            company: None,
            home: None,
            tags: None,
        },
    ];

    let mut builders = Person::new_builders(people.len());
    builders.append_rows(people);
    db.ingest(builders.finish().into_record_batch()).await?;

    println!("Inserted 3 people with deep nested data\n");

    // Query and traverse the nested structure
    let batches = db.scan().collect().await?;

    println!("=== Deep Nested Data ===\n");
    for batch in &batches {
        for person in batch.iter_views::<Person>()?.try_flatten()? {
            println!("Person {} - {}", person.id, person.name);

            // Traverse: company -> hq -> geo (3 levels deep)
            match person.company {
                Some(company) => {
                    println!("  Company: {}", company.name);
                    match company.hq {
                        Some(hq) => {
                            print!("    HQ: {}", hq.city);
                            if let Some(zip) = hq.zip {
                                print!(", {}", zip);
                            }
                            if let Some(geo) = hq.geo {
                                print!(" ({:.4}, {:.4})", geo.lat, geo.lon);
                            }
                            println!();
                        }
                        None => println!("    HQ: (remote)"),
                    }
                }
                None => println!("  Company: (none)"),
            }

            // Home address with optional geo
            match person.home {
                Some(home) => {
                    print!("  Home: {}", home.city);
                    if let Some(geo) = home.geo {
                        print!(" ({:.4}, {:.4})", geo.lat, geo.lon);
                    }
                    println!();
                }
                None => println!("  Home: (none)"),
            }

            // Tags list
            match person.tags {
                Some(tags) => {
                    let vals: Vec<String> = tags
                        .map(|r| r.map(|s| s.to_string()))
                        .collect::<Result<_, _>>()?;
                    println!("  Tags: {:?}", vals);
                }
                None => println!("  Tags: (none)"),
            }

            println!();
        }
    }

    Ok(())
}
