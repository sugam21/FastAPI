async function SubmitGetRequest() {
    const customer_id = document.getElementById("get-account-id")
    console.log(customer_id.value)
    // console.log(JSON.stringify(formData))
    const response = await fetch(`http://127.0.0.1:8000/get_customer_info/${customer_id.value}]}`,
        {
            method: "GET",
            headers: { "Content-type": "application/json; charset=UTF-8" }
        })
        const responseText = await response.text();
        console.log(responseText); // logs 'OK'
}