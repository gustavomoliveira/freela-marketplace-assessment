package br.com.freela.contrato.infrastructure.web;

import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/contratos")
public class ContratoController {
    private static final Logger log = LoggerFactory.getLogger(ContratoController.class);
    private final ContratoApplicationService service;

    public ContratoController(ContratoApplicationService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ContratoResponse criar(@Valid @RequestBody CriarContratoRequest request) {
        log.info("http.contrato.criar clienteId={} freelancerId={}", request.clienteId(), request.freelancerId());
        var c = service.criar(new CriarContratoCommand(request.clienteId(), request.freelancerId(), request.titulo(), request.valor()));
        log.info("http.contrato.criar.response contratoId={} status={}", c.id(), c.status());
        return ContratoResponse.from(c);
    }

    @PostMapping("/{id}/entrega")
    public ContratoResponse registrarEntrega(@PathVariable UUID id) {
        log.info("http.contrato.entrega contratoId={}", id);
        var c = service.registrarEntrega(id);
        log.info("http.contrato.entrega.response contratoId={} status={}", c.id(), c.status());
        return ContratoResponse.from(c);
    }

    @PostMapping("/{id}/conclusao")
    public ContratoResponse concluir(@PathVariable UUID id) {
        log.info("http.contrato.conclusao contratoId={}", id);
        var c = service.concluir(id);
        log.info("http.contrato.conclusao.response contratoId={} status={}", c.id(), c.status());
        return ContratoResponse.from(c);
    }

    @PostMapping("/{id}/cancelamento")
    public ContratoResponse cancelar(@PathVariable UUID id) {
        log.info("http.contrato.cancelamento contratoId={}", id);
        var c = service.cancelar(id);
        log.info("http.contrato.cancelamento.response contratoId={} status={}", c.id(), c.status());
        return ContratoResponse.from(c);
    }

    @GetMapping("/{id}")
    public ContratoResponse buscar(@PathVariable UUID id) {
        log.info("http.contrato.buscar contratoId={}", id);
        return ContratoResponse.from(service.buscar(id));
    }

    @GetMapping
    public List<ContratoResponse> listar() {
        log.info("http.contrato.listar");
        return service.listar().stream().map(ContratoResponse::from).toList();
    }
}