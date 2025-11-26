async function deactivate_ts(id) {
  const ts_element = document.getElementById("timeseries-" + id.toString());
  const button = ts_element.getElementsByClassName("deactivate-ts")[0];

  button.style.background = "gray";

  try {
    const response = await fetch("/cms/deactivate_ts?id=" + id.toString(), { method: "POST" });
    if (!response.ok) {
      throw new Error(`Failed deactivation request: ${response.status} ${response.body}`);
    }

    button.style.background = "green";
  } catch (error) {
    // TODO: show error message in UI somehow?
    console.error(error.message);

    button.style.background = "red";
  }
}
