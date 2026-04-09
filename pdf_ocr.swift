import AppKit
import Foundation
import PDFKit
import Vision

enum OCRError: Error {
    case badArguments
    case openFailed
    case renderFailed(Int)
}

func renderPage(_ page: PDFPage, scale: CGFloat) throws -> CGImage {
    let bounds = page.bounds(for: .mediaBox)
    let width = max(Int(bounds.width * scale), 1)
    let height = max(Int(bounds.height * scale), 1)
    guard let ctx = CGContext(
        data: nil,
        width: width,
        height: height,
        bitsPerComponent: 8,
        bytesPerRow: 0,
        space: CGColorSpaceCreateDeviceRGB(),
        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
    ) else {
        throw OCRError.renderFailed(0)
    }

    ctx.setFillColor(NSColor.white.cgColor)
    ctx.fill(CGRect(x: 0, y: 0, width: width, height: height))
    ctx.saveGState()
    ctx.translateBy(x: 0, y: CGFloat(height))
    ctx.scaleBy(x: scale, y: -scale)
    page.draw(with: .mediaBox, to: ctx)
    ctx.restoreGState()

    guard let image = ctx.makeImage() else {
        throw OCRError.renderFailed(0)
    }
    return image
}

func recognizeText(from image: CGImage) throws -> String {
    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = true
    request.recognitionLanguages = ["en-US"]

    let handler = VNImageRequestHandler(cgImage: image, options: [:])
    try handler.perform([request])

    let observations = request.results ?? []
    let ordered = observations.sorted {
        if abs($0.boundingBox.midY - $1.boundingBox.midY) > 0.02 {
            return $0.boundingBox.midY > $1.boundingBox.midY
        }
        return $0.boundingBox.minX < $1.boundingBox.minX
    }
    return ordered.compactMap { $0.topCandidates(1).first?.string }.joined(separator: "\n")
}

let args = CommandLine.arguments
guard args.count >= 2 else {
    fputs("usage: pdf_ocr.swift <pdf-path> [max-pages]\n", stderr)
    throw OCRError.badArguments
}

let pdfPath = args[1]
let maxPages = args.count >= 3 ? max(Int(args[2]) ?? 0, 0) : 0

guard let doc = PDFDocument(url: URL(fileURLWithPath: pdfPath)) else {
    throw OCRError.openFailed
}

var chunks: [String] = []
let totalPages = doc.pageCount
let pageLimit = maxPages > 0 ? min(maxPages, totalPages) : totalPages

for idx in 0..<pageLimit {
    autoreleasepool {
        guard let page = doc.page(at: idx) else { return }
        do {
            let image = try renderPage(page, scale: 2.0)
            let text = try recognizeText(from: image).trimmingCharacters(in: .whitespacesAndNewlines)
            if !text.isEmpty {
                chunks.append(text)
            }
        } catch {
            fputs("page \(idx + 1): \(error)\n", stderr)
        }
    }
}

FileHandle.standardOutput.write((chunks.joined(separator: "\n\n") + "\n").data(using: .utf8)!)