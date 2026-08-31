# AI tools

### Claude Web
I use it as google, so I asked it to refresh my memory on database locks, tokio-rs internals, etc.

### Claude Code
100% of code you see in this repository was implemented by prompting in claude code (model - Opus 5). But I did >90% of the planning of architecture, schemas, SQL transactions, etc - The only time I used claude code to plan a subsystem design was for webhook integration which I did at the end also because I was running out of time.

# 3 Self made decisions

1. Postgres schema for customers, invoices, payment intents. (Did not consult AI)

2. Defined Invoice states and transition SQL statements. (Semi consulted AI)

To be fair, I did all of the thinking and wrote down almost all the invoice SQL statments but it wasn't gramatically correct. It was mixed with english. I really asserted myself places where I needed FOR UPDATE locks and things like that. (I have a knack of where it can go wrong) So I told claude code to formalize those and then implement.

3. Decision to make Business API Key an environment variable, because it becomes easy to revoke/change. (Just redeploy) For this AI suggested a whole set of crud endpoints to store API keys on postgres, etc which was an overkill.


# 1 thing AI got wrong

In the payment service provider, the lifetime of the settlement task was tied to the lifetime of the request. It is funny because I anticipated this before I even opened claude code. It didn't take me long to notice that `tokio::spawn` wasn't used so the client's request timedout then the settlement logic might not happen. That is obviously not what we want so I decoupled the lifetimes of the handler from the actual settlement process.
