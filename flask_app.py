from flask import Flask, request, render_template, make_response, redirect, url_for
import recomendar
import sys

app = Flask(__name__)

@app.route('/', methods=('GET', 'POST'))
def login():
    if request.method == 'POST' and 'username' in request.form:
        user = request.form['username']
        recomendar.crear_usuario(user)
        res = make_response(redirect("/recomendaciones"))
        res.set_cookie('user', user)
        return res

    if request.method == 'GET' and 'user' in request.cookies:
        return make_response(redirect("/recomendaciones"))

    return render_template('login.html')

@app.route('/recomendaciones', methods=['GET', 'POST'])
def recomendaciones():
    user = request.cookies.get('user')

    if request.method == 'POST':
        for bg_name in request.form.keys():
            rating = int(request.form[bg_name])
            recomendar.insertar_interacciones(bg_name, user, rating)

    bg_games, metodo_utilizado = recomendar.recomendar(user)

    for bg_game in bg_games:
        recomendar.insertar_interacciones(bg_game["id"], user, 0)

    cant_valorados = len(recomendar.valorados(user))
    cant_ignorados = len(recomendar.ignorados(user))

    return render_template("recomendaciones.html", games=bg_games, user=user, cant_valorados=cant_valorados, cant_ignorados=cant_ignorados, metodo_utilizado=metodo_utilizado)

@app.route('/logout', methods=['POST'])
def logout():
    if 'user' in request.cookies:
        response = redirect(url_for('login'))
        response.delete_cookie('user')
        return response
    else:
        return redirect(url_for('login'))

@app.route('/reset', methods=['POST'])
def reset():
    user = request.cookies.get('user')
    recomendar.reset_usuario(user)
    return make_response(redirect("/recomendaciones"))

if __name__ == "__main__":
    app.run(debug=True)