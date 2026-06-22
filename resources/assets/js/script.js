async function set_ts_activation(id, deactivate) {
  const ts_element = document.getElementById("timeseries-" + id.toString());
  const button = ts_element.getElementsByClassName("ts-activation")[0];

  button.style.background = "gray";

  try {
    let url = "/cms/set_ts_activation?id=" + id.toString();
    if (deactivate) {
      url += "&deactivated=true";
    }

    const response = await fetch(url, { method: "POST" });
    if (!response.ok) {
      throw new Error(`Failed set_ts_activation request: ${response.status} ${response.body}`);
    }

    button.style.background = "green";
  } catch (error) {
    // TODO: show error message in UI somehow?
    console.error(error.message);

    button.style.background = "red";
  }
}
