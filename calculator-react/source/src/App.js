import React, { useState } from "react"
import swal from "@sweetalert/with-react"
import "./App.css"

export default function App() {
    const [num1, setNum1] = useState(0)
    const [op, setOp] = useState("+")
    const [num2, setNum2] = useState(0)
    const [ans, setAns] = useState(0)

    function handleSubmit(e) {
        // avoid refreshing
        e.preventDefault()

        const requestOptions = {
            headers: { "Content-Type": "application/json" },
            method: "POST",
            body: JSON.stringify({
                Num1: Number(num1),
                OP: op,
                Num2: Number(num2),
            }),
        }

        fetch("/api", requestOptions)
            .then(res => res.json())
            .then(json => {
                setAns(json.ans)
                swal("Answer: ", json.ans.toString(), "success")
            })
    }

    return (
        <div className="app">
		    <img src="omni-logo.svg" align="center"/>
            <form className="calculation-form" onSubmit={e => handleSubmit(e)}>
                <div className="input-field">
                    <label>Input Numbers1</label>
                    <input
                        type="text"
                        value={num1}
                        onChange={e => setNum1(e.target.value)}
                    ></input>
                    <label>Operator</label>
                    <input
                        type="text"
                        value={op}
                        onChange={e => setOp(e.target.value)}
                    ></input>
                    <label>Input Numbers2:</label>
                    <input
                        type="text"
                        value={num2}
                        onChange={e => setNum2(e.target.value)}
                    ></input>
                </div>
                <input
                    type="submit"
                    value="Calculate Demo"
                    className="button"
                ></input>
            </form>
            <label>Answer</label>
            <input
                type="text"
                value={ans}
                onChange={e => setAns(e.target.value)}
            ></input>
        </div>
    )
}
