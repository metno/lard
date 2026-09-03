pub async fn ensure_oidc_auth() {
    let http_client = reqwest::Client::builder()
        // So we can manually inspect the redirects and see they're what we expect before
        // following
        .redirect(reqwest::redirect::Policy::none())
        // We need the session cookies to persist between requests for the auth flow
        .cookie_store(true)
        .build()
        .unwrap();

    let cms_url = "http://localhost:3001/cms";
    // user tries to visit the CMS with no auth
    let cms_resp = http_client.get(cms_url).send().await.unwrap();
    let auth_url = cms_resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap();
    let (auth_url_base, _) = auth_url.split_once('?').unwrap();
    // the request is intercepted by the auth middleware, which sees the
    // user doesn't have auth and redirects them to the provider's
    // /authorize endpoint after setting up a challenge and embedding its
    // params in the query string
    assert_eq!(cms_resp.status(), reqwest::StatusCode::SEE_OTHER);
    assert_eq!(auth_url_base, "http://localhost:3008/authorize");

    // user follows the redirect to provider/authorize
    let auth_resp = http_client.get(auth_url).send().await.unwrap();
    let redirect_url = auth_resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap();
    let (redirect_url_base, _) = redirect_url.split_once('?').unwrap();
    // once the provider has authenticated the user (weird that that happens
    // at an endpoint called authorize???) it redirects the user to our oidc
    // redirect handler
    assert_eq!(auth_resp.status(), reqwest::StatusCode::SEE_OTHER);
    assert_eq!(redirect_url_base, "http://localhost:3001/oidc_redirect");

    // user follows the redirect to ingestion/oidc_redirect
    // Note: this endpoint internally makes the call to token_url, then
    // shoves the permissions from the token into a cookie
    let redirect_resp = http_client.get(redirect_url).send().await.unwrap();
    let cms_url2 = redirect_resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap();
    // now the flow is complete and the user can be redirected back to what
    // they were originally trying to access, in this case the CMS
    assert_eq!(auth_resp.status(), reqwest::StatusCode::SEE_OTHER);
    // for some reason axum doesn't include the host in its OriginalUri
    // extractor, that's why it's missing here, but i think it's fine as we
    // are being redirected from ingestion to ingestion
    assert_eq!(cms_url2, "/cms");

    // bake the host in, because even though a client won't need it in
    // practice, reqwest does need it
    let cms_url2 = format!("http://localhost:3001{}", cms_url2);
    // user follows the redirect back to the CMS
    let cms_resp2 = http_client.get(&cms_url2).send().await.unwrap();
    // this time the auth middleware sees that the user has an cookie with
    // the right permission to access the cms, so lets the request through
    assert_eq!(cms_resp2.status(), reqwest::StatusCode::OK);

    eprintln!("oidc_auth ok");
}
