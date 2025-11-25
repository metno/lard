function deactivate_ts(id) {
  const ts_element = document.getElementById("timeseries-" + id.toString());
  const button = ts_element.getElementsByClassName("deactivate-ts")[0];

  button.style.color = "gray";

  try {
    const response = await fetch("/cms/deactivate_ts" { method: "POST" });
    if (!response.ok) {
      throw new Error(`Failed deactivation request: ${response.status} ${response.body}`);
    }

    button.style.color = "green";
  } catch (error) {
    // TODO: show error message in UI somehow?
    console.error(error.message);

    button.style.color = "red";
  }
}
