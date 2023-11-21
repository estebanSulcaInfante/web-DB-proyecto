from flask import Flask, render_template
import psycopg2

app = Flask(__name__)

# Configuración de la conexión a la base de datos
def get_db_connection():
    conn = psycopg2.connect(
        host='localhost',
        dbname='proyectofinal',
        user='postgres',
    )
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/consulta1')
def consulta1():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
                SELECT codigo, SUM(cantidad) FROM
    ((SELECT PC.ProductoBaseCodigo AS codigo, SUM(T.cantidad) AS cantidad
        FROM ProductoCotizado PC
        INNER JOIN tiene T ON PC.codigo = T.ProductoCodigo
    GROUP BY PC.ProductoBaseCodigo)
    UNION ALL
    (SELECT PB.codigo, SUM(T.cantidad) AS cantidad
        FROM productobase PB
        INNER JOIN tiene T ON PB.codigo = T.ProductoCodigo
    GROUP BY PB.codigo)) AS VentasProductos
GROUP BY codigo ORDER BY SUM (cantidad) DESC LIMIT 10;
                ''')
    resultados = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('consulta1.html', resultados=resultados)

@app.route('/consulta2')
def consulta2():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        WITH Costos AS (
  SELECT
    ProductoBase.Codigo AS CodigoProducto,
    COALESCE(SUM(requiere.Cantidad * Materiaprima.ValorUnitario), 0) AS CostoTotal
  FROM ProductoBase
  LEFT JOIN Requiere ON ProductoBase.Codigo = Requiere.productobasecodigo
  LEFT JOIN MateriaPrima ON Requiere.materiaprimacodigo = MateriaPrima.Codigo
  GROUP BY ProductoBase.Codigo
),
VentasBase AS (
  SELECT
    Tiene.productocodigo AS CodigoProducto,
    COUNT(DISTINCT Venta.Codigo) AS VentasRealizadas,
    COALESCE(SUM(Tiene.Cantidad * ProductoBase.PrecioUnitario), 0) AS IngresosVentasBase
  FROM Tiene
  LEFT JOIN Venta ON Tiene.ventacodigo = Venta.Codigo
  LEFT JOIN ProductoBase ON Tiene.productocodigo = ProductoBase.Codigo
  GROUP BY Tiene.productocodigo
),
VentasCotizadas AS (
  SELECT
    ProductoCotizado.productobasecodigo AS CodigoProducto,
    COUNT(DISTINCT Venta.Codigo) AS VentasRealizadas,
    COALESCE(SUM(Tiene.Cantidad * ProductoCotizado.NuevoPrecioUnitario), 0) AS IngresosVentasCotizadas
  FROM ProductoCotizado
  LEFT JOIN Tiene ON ProductoCotizado.Codigo = Tiene.productocodigo
  LEFT JOIN Venta ON Tiene.ventacodigo = Venta.Codigo
  GROUP BY ProductoCotizado.productobasecodigo
),
Rentabilidad AS (
  SELECT
    ProductoBase.codigo,
    ProductoBase.nombre,
    CostoTotal,
    COALESCE(VentasBase.VentasRealizadas, 0) + COALESCE(VentasCotizadas.VentasRealizadas, 0) AS VentasRealizadas,
    COALESCE(IngresosVentasBase, 0) + COALESCE(IngresosVentasCotizadas, 0) AS IngresosTotales,
    (COALESCE(IngresosVentasBase, 0) + COALESCE(IngresosVentasCotizadas, 0)) - CostoTotal AS GananciaNeta
  FROM ProductoBase
  LEFT JOIN Costos ON ProductoBase.Codigo = Costos.CodigoProducto
  LEFT JOIN VentasBase ON ProductoBase.Codigo = VentasBase.CodigoProducto
  LEFT JOIN VentasCotizadas ON ProductoBase.Codigo = VentasCotizadas.CodigoProducto
)
SELECT
  Codigo,
  Nombre,
  CostoTotal,
  VentasRealizadas,
  IngresosTotales,
  GananciaNeta
FROM Rentabilidad;

        ''')  # Asegúrate de reemplazar esto con tu consulta SQL real
    resultados = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('consulta2.html', resultados=resultados)

@app.route('/consulta3')
def consulta3():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
  WITH MateriaPrimaUtilizada AS (
    SELECT
      Requiere.MateriaPrimaCodigo,
      SUM(Requiere.Cantidad) AS CantidadUtilizada
    FROM Requiere
    INNER JOIN Produce ON Requiere.ProductoBaseCodigo = Produce.ProductoBaseCodigo
    GROUP BY Requiere.MateriaPrimaCodigo
  ),
  ProductosMasVendidos AS (
    SELECT
      Tiene.ProductoCodigo,
      SUM(Tiene.Cantidad) AS CantidadVendida
    FROM Tiene
    GROUP BY Tiene.ProductoCodigo
    ORDER BY CantidadVendida DESC
    LIMIT 10
  )
  SELECT
    MateriaPrima.Codigo,
    MateriaPrima.Nombre,
    MateriaPrima.Stock,
    MateriaPrimaUtilizada.CantidadUtilizada
  FROM MateriaPrima
  INNER JOIN MateriaPrimaUtilizada ON MateriaPrima.Codigo = MateriaPrimaUtilizada.MateriaPrimaCodigo
  WHERE MateriaPrima.Codigo IN (
    SELECT Requiere.MateriaPrimaCodigo
    FROM Requiere
    WHERE Requiere.ProductoBaseCodigo IN (
      SELECT ProductoBase.Codigo
      FROM ProductoBase
      INNER JOIN ProductosMasVendidos ON ProductoBase.Codigo = ProductosMasVendidos.ProductoCodigo
    )
  )
  ORDER BY MateriaPrima.Stock, MateriaPrimaUtilizada.CantidadUtilizada DESC;
        ''')  # Asegúrate de reemplazar esto con tu consulta SQL real
    resultados = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('consulta3.html', resultados=resultados)


if __name__ == '__main__':
    app.run(debug=True)
