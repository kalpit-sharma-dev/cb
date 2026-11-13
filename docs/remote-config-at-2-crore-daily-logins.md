# Remote Config at 2 crore daily logins

Two ways to have Firebase Remote Config values on mobile app startup for a base of 5.5 crore customers and 2 crore daily logins. Inactive customers do not create fetches. Cost follows logins.

| | |
|---|---|
| Registered customers | 5.5 crore |
| Daily logins | 2 crore |
| Free Remote Config fetches | 1 lakh per day |
| Daily Firebase bill | About $69 (on-device SDK) or $0 (login cache) |

Ship the login cache when the Remote Config invoice must stay at zero. Ship the on-device SDK when A/B tests, personalization, and Firebase conditions must keep working, and about $69 a day is acceptable.

Pricing below is from Firebase Remote Config, effective 1 September 2026. The first 1,00,000 fetches each day are free, then $0.06 per 10,000 up to 1 crore, then $0.01 per 10,000. A cached `activate()` or `get` does not count. A template read through the REST API or an Admin SDK does count. Existing projects are billed from 1 December 2026 on Spark and from 1 February 2027 on Blaze.

## Approach 1 — On-device SDK

The phone paints startup from the last activated values on disk. After the first frame it fetches in the background, at most once every 12 hours. Firebase targeting, rollouts, A/B tests, and personalization stay on the client SDK.

A published value shows up on the next cold start after that background fetch. With one login a day, that is the following day.

At one network fetch per daily login, 2 crore downloads cost about $69 a day:

- 1,00,000 free
- 99,00,000 at $0.000006 = $59.40
- 1,00,00,000 at $0.000001 = $10.00

A second fetch, when a session crosses the 12-hour boundary, is about $89 a day. A network fetch on every open, at three opens a day, is about $109 a day, and the first screen waits on the radio.

### Startup order

1. Ship a default for every parameter in the app binary.
2. In release builds set the minimum fetch interval to 12 hours (43,200 seconds). Use 24 hours for values that rarely change. Keep a zero interval in debug builds only.
3. On cold start call `activate()`, apply those values, and draw the first screen. This is a local read.
4. After the first frame call `fetch()` and do not wait for it. The SDK contacts Firebase only when the interval has elapsed. That download is the billable fetch.
5. Later opens the same day are cache hits and add nothing to the bill.

A realtime listener is free while it is only connected. One publish while 2 crore sessions are listening is about 2 crore downloads, another $69, in one burst. Reserve it for a rare incident switch.

## Approach 2 — Login version cache

One job on the login service reads the client template every 5 minutes and keeps it in process memory. Login carries a version integer, and the full parameter block only when that version changed. The app saves it and paints the next startup from disk.

288 template reads a day sit inside the free 1 lakh. There is no new config API and no Aerospike. Days when nobody publishes a change, login grows by an integer.

### Server

1. One refresher calls the client template endpoint `projects.remoteConfig.get` every 5 minutes. That is 288 reads a day (`24 × 60 / 5`). Keep the document in process memory on the login service. If a refresh fails, keep serving the last good copy.
2. Every login response includes `configVersion`. Include the parameter block only when the device’s version is older.
3. While building that block, apply the conditions you still need: app version, platform, and a stable user or installation id. Percentage conditions fall through to the in-app default when that id is missing.

`getServerTemplate()` loads a different document from the mobile client template. The refresher reads the client template.

288 reads holds only if a single job polls Firebase. Independent polls from every pod multiply that by the pod count. About 350 pods polling every 5 minutes would cross the free 1 lakh.

### App

4. On cold start, apply the saved payload and draw the UI. When login returns a newer version, save it. The next cold start paints that copy with no extra round trip.
5. First install and a logged-out start have no payload yet. Use the defaults in the binary until the first successful login.

## Side by side

| | On-device SDK | Login version cache |
|---|---|---|
| Firebase bill | About $69/day at one fetch per login | $0 (288 server reads) |
| Startup | Disk read. The first screen does not wait on the network. | Disk read of the last login payload. |
| Freshness | Next cold start after the background fetch. | Current login. The following cold start already has it. |
| Targeting and experiments | App version, country, language, percent rollout, A/B tests, and personalization stay in the SDK. | Evaluate app version, platform, and a stable user id while building the login response. |
| What you add | A 12-hour minimum fetch interval and a split activate-then-fetch on startup. | A version field on login, and one in-memory template on the login service. |
| When Firebase is down | Last activated values on the device, then in-app defaults. | Last good template in memory, then the last payload on disk, then in-app defaults. |

## Daily Firebase bill

USD per day at 2 crore daily logins. One fetch per login is 2 crore billable downloads.

| Pattern | Daily Firebase bill |
|---|---|
| Login cache, 288 server reads | $0 |
| SDK, one fetch per daily login | $69.40 |
| SDK, two fetches (12-hour boundary) | $89.40 |
| SDK, network fetch on all 3 opens | $109.40 |

## Leave these out

| Option | Why it falls away at 2 crore daily logins |
|---|---|
| New config API on every startup | Moves 2 crore requests a day onto the load balancer and app servers. Startup waits on a call the disk read does not need. |
| Aerospike for the template | The template is one small document. Process memory on the login service holds it. |
| Client poll every 5 minutes | The 5-minute timer belongs on the one server refresher. A phone poll at that rate is a large bill and a synchronized spike. |
| Firebase Hosting of a public JSON file | Free Hosting transfer is about 360 MB a day. A 1 KB file for 2 crore opens is about 20 GB. |
| FCM as the source of truth | FCM has no per-message fee, and the send quota is 6 lakh messages a minute, so one update takes about half an hour to fan out. Delivery is unreliable once the app process is gone. |
| Admin server template | `getServerTemplate()` loads a different document from the mobile client template. |

FCM can still nudge a live session to log in again. The payload written at login is the copy startup reads.

Firebase Hosting and FCM quotas are from Firebase pricing and FCM throttling docs, checked November 2026.
